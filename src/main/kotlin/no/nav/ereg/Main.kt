package no.nav.ereg

import mu.KotlinLogging

fun main() {

    val log = KotlinLogging.logger {}

    log.info { "Starting" }

    log.info { "Checking environment variables" }
    EnvVarFactory.envVar.let { ev ->

        if (!ev.eregDetailsComplete() || !ev.proxyDetailsComplete()) {
            log.error { "EREG - or proxy details are incomplete - " }
            return
        }

        log.info { "Proxy details: ${ev.httpsProxy} with port ${ev.noProxy}" }

        if (ev.kafkaSecurityEnabled() && !ev.kafkaSecurityComplete()) {
            log.error { "Kafka security enabled, but incomplete kafka security properties - " }
            return
        }
    }

    Bootstrap.start()

    log.info { "Finished!" }
}
