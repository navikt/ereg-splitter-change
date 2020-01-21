package no.nav.ereg

import mu.KotlinLogging

fun main() {

    val log = KotlinLogging.logger {}

    log.info { "Starting" }

    log.info { "Checking environment variables" }
    EnvVarFactory.envVar.let { ev ->

        if (!ev.eregDetailsComplete()) {
            log.error { "EREG details are incomplete - " }
            return
        }

        if (ev.kafkaSecurityEnabled() && !ev.kafkaSecurityComplete()) {
            log.error { "Kafka security enabled, but incomplete kafka security properties - " }
            return
        }
    }

    Bootstrap.start()

    log.info { "Finished!" }
}
