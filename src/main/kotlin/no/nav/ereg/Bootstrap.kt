package no.nav.ereg

import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.ereg.ServerState.state

object Bootstrap {

    private val log = KotlinLogging.logger { }

    fun start(ev: EnvVar = EnvVarFactory.envVar) {

        ShutdownHook.reset()

        NaisDSL.enabled { conditionalSchedule(ev) } // end of use for NAISDSL - shutting down REST API
    }

    private tailrec fun conditionalSchedule(ev: EnvVar) {

        // some resets before next attempt/work session
        ServerState.reset()
        Metrics.sessionReset()
        work(ev) // ServerState will be updated according to any issues

        if (!ServerState.isOk()) {
            Metrics.noOfAttempts.inc()
            if (!ShutdownHook.isActive() && Metrics.noOfAttempts.get().toInt() < ev.maxAttempts)
                waitByState(ev)
        } else {
            Metrics.noOfAttempts.clear()
            if (!ShutdownHook.isActive() && ev.runEachMorning()) waitByState(ev)
        }

        if (!ShutdownHook.isActive() &&
            (Metrics.noOfAttempts.get().toInt() < ev.maxAttempts && !ServerState.isOk()) ||
            (ev.runEachMorning() && ServerState.isOk())) conditionalSchedule(ev)
    }

    private fun waitByState(ev: EnvVar) {
        val msDelay = when (state) {
            // tomorrow at 05:30
            is ServerStates.IsOk -> if (ev.runEachMorning()) getTomorrowMorning() else 1_000
            // 15 min
            else -> ev.msBetweenAttempts
        }
        log.info { "Will wait $msDelay ms before starting all over" }
        runCatching { runBlocking { delay(msDelay) } }
            .onSuccess { log.info { "waiting completed" } }
            .onFailure { log.info { "waiting interrupted" } }
    }

    private fun getTomorrowMorning() = Duration.between(
        LocalDateTime.now(),
        LocalDateTime.parse(LocalDate.now().plusDays(1).toString() + "T05:30:00"))
        .toMillis()
}
