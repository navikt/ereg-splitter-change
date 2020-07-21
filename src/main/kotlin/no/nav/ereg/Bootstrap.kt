package no.nav.ereg

import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.PrestopHook
import no.nav.sf.library.enableNAISAPI

private const val EV_bootstrapRetryWaitTime = "MS_BETWEEN_RETRIES"
private const val EV_bootstrapRunEachMorning = "RUN_EACH_MORNING"
private const val EV_bootstrapMaxAttempts = "MAX_ATTEMPTS"

private val bootstrapRetryWaitTime = AnEnvironment.getEnvOrDefault(EV_bootstrapRetryWaitTime, "1800000").toLong()
private val bootstrapRunEachMorning = AnEnvironment.getEnvOrDefault(EV_bootstrapRunEachMorning, "FALSE") == "TRUE"
private val bootstrapMaxAttempts = AnEnvironment.getEnvOrDefault(EV_bootstrapMaxAttempts, "24").toInt()

object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start(ws: WorkSettings = WorkSettings()) {
        log.info { "Starting" }
        enableNAISAPI { loop(ws) }
        log.info { "Finished!" }
    }

    private tailrec fun loop(ws: WorkSettings) {
        log.info { "Ready to loop" }
        val stop = no.nav.sf.library.ShutdownHook.isActive() || PrestopHook.isActive()
        when {
            stop -> Unit
            !stop -> {
                log.info { "Continue to loop" }
                val result = work(ws)
                if (result.second.isOK()) {
                    val delay = if (bootstrapRunEachMorning) getTomorrowMorning() else 1_000
                    conditionalWait(delay)
                    loop(result.first)
                } else {
                    Metrics.noOfAttempts.inc()
                    log.info { "Failed attempt ${Metrics.noOfAttempts.get().toInt()}/$bootstrapMaxAttempts" }
                    if (Metrics.noOfAttempts.get().toInt() < bootstrapMaxAttempts) {
                        conditionalWait(bootstrapRetryWaitTime)
                        loop(result.first)
                    }
                }
            }
        }
    }

    private fun getTomorrowMorning() = Duration.between(
        LocalDateTime.now(),
        LocalDateTime.parse(LocalDate.now().plusDays(1).toString() + "T05:30:00"))
        .toMillis()

    private fun conditionalWait(ms: Long) =
        runBlocking {
            log.info { "Will wait $ms ms before starting all over" }

            val cr = launch {
                runCatching { delay(ms) }
                    .onSuccess { log.info { "waiting completed" } }
                    .onFailure { log.info { "waiting interrupted" } }
            }

            tailrec suspend fun loop(): Unit = when {
                cr.isCompleted -> Unit
                no.nav.sf.library.ShutdownHook.isActive() || PrestopHook.isActive() -> cr.cancel()
                else -> {
                    delay(250L)
                    loop()
                }
            }

            loop()
            cr.join()
        }
}
