package no.nav.ereg

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.ereg.nais.PrestopHook
import no.nav.ereg.nais.enableNAISAPI
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime

private const val EV_MS_BETWEEN_RETRIES = "MS_BETWEEN_RETRIES"
private const val EV_RUN_EACH_MORNING = "RUN_EACH_MORNING"
private const val EV_MAX_ATTEMPTS = "MAX_ATTEMPTS"

private val bootstrapRetryWaitTime = getEnvOrDefault(EV_MS_BETWEEN_RETRIES, "1800000").toLong()
private val bootstrapRunEachMorning = getEnvOrDefault(EV_RUN_EACH_MORNING, "FALSE") == "TRUE"
private val bootstrapMaxAttempts = getEnvOrDefault(EV_MAX_ATTEMPTS, "24").toInt()

object Bootstrap {
    private val log = KotlinLogging.logger { }

    fun start(ws: WorkSettings = WorkSettings()) {
        log.info { "Starting" }
        enableNAISAPI {
            loop(ws)
        }
        log.info { "Finished!" }
    }

    private tailrec fun loop(ws: WorkSettings) {
        log.info { "Ready to loop" }
        val stop = PrestopHook.isActive()
        when {
            stop -> Unit
            !stop -> {
                log.info { "Continue to loop" }

                Metrics.sessionReset()
                val result = work(ws)

                if (result.second.isOK()) {
                    val delay = if (bootstrapRunEachMorning) getTomorrowMorning() else 1_000
                    conditionalWait(delay)
                    loop(result.first)
                } else {
                    Metrics.noOfAttempts.inc() // TODO Replace this with worksettings variable?
                    log.info { "Failed attempt ${Metrics.noOfAttempts.get().toInt()}/$bootstrapMaxAttempts" }
                    if (Metrics.noOfAttempts.get().toInt() < bootstrapMaxAttempts) {
                        conditionalWait(bootstrapRetryWaitTime)
                        loop(result.first)
                    }
                }
            }
        }
    }

    private fun getTomorrowMorning() =
        Duration
            .between(
                LocalDateTime.now(),
                LocalDateTime.parse(LocalDate.now().plusDays(1).toString() + "T05:30:00"),
            ).toMillis()

    fun conditionalWait(ms: Long) =
        runBlocking {
            log.info { "Will wait $ms ms before starting all over" }

            val cr =
                launch {
                    runCatching { delay(ms) }
                        .onSuccess { log.info { "waiting completed" } }
                        .onFailure { log.info { "waiting interrupted" } }
                }

            tailrec suspend fun loop(): Unit =
                when {
                    cr.isCompleted -> Unit
                    PrestopHook.isActive() -> cr.cancel()
                    else -> {
                        delay(250L)
                        loop()
                    }
                }

            loop()
            cr.join()
        }
}
