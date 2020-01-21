package no.nav.ereg

import io.prometheus.client.exporter.common.TextFormat
import java.io.IOException
import java.io.StringWriter
import mu.KotlinLogging
import no.nav.ereg.Metrics.cRegistry
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.server.Netty
import org.http4k.server.asServer

object NaisDSL {

    private val log = KotlinLogging.logger { }

    const val ISALIVE = "/isAlive"
    const val ISREADY = "/isReady"
    const val METRICS = "/metrics"

    private val api = routes(
        ISALIVE bind Method.GET to { Response(Status.OK).body("is alive") },
        ISREADY bind Method.GET to { Response(Status.OK).body("is ready") },
        METRICS bind Method.GET to {
            val content = try {
                StringWriter().let { str ->
                    TextFormat.write004(str, cRegistry.metricFamilySamples())
                    str
                }.toString()
            } catch (e: IOException) {
                log.error { "/prometheus failed writing metrics - ${e.message}" }
                ""
            }
            if (content.isNotEmpty()) Response(Status.OK).body(content)
            else Response(Status.NO_CONTENT).body(content)
        }
    ).asServer(Netty(8080))

    fun enabled(doSomething: () -> Unit) = api.use {
        try {
            it.start()
            log.info { "NAIS DSL is up and running" }
            doSomething()
            log.info { "NAIS DSL is stopped" }
        } catch (e: Exception) {
            log.error { "Could not enable NAIS api for port 8080" }
        } finally {
            it.stop()
        }
    }
}
