package no.nav.ereg

import java.io.File
import mu.KotlinLogging
import org.http4k.core.Body
import org.http4k.core.Method
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.filter.gzipped
import org.http4k.routing.bind
import org.http4k.routing.routes
import org.http4k.server.Jetty
import org.http4k.server.asServer

private val log = KotlinLogging.logger {}

internal const val FILUENHETER = "./src/test/resources/underenheter_test.json"
internal const val FILUENHETERCHANGED = "./src/test/resources/underenheter_changed_test.json"
internal const val FILOENHETER = "./src/test/resources/enheter_test.json"
internal const val FILOENHETERCHANGED = "./src/test/resources/enheter_changed_test.json"

internal fun eregAPI(
    port: Int,
    filUEnheter: String = FILUENHETER,
    filOEnheter: String = FILOENHETER,
    doSomething: (String, String) -> Unit
) {

    val ueEndpoint = "/underenheter"
    val ueURL = "http://localhost:$port$ueEndpoint"
    val oeEndpoint = "/enheter"
    val oeURL = "http://localhost:$port$oeEndpoint"

    routes(
        ueEndpoint bind Method.GET to { Response(Status.OK).body(Body(File(filUEnheter).inputStream()).gzipped().body) },
        oeEndpoint bind Method.GET to { Response(Status.OK).body(Body(File(filOEnheter).inputStream()).gzipped().body) }
    ).asServer(Jetty(port)).use { srv ->
        try {
            Response(Status.OK).body(Body(File(filOEnheter).inputStream()))
            srv.start()
            doSomething(ueURL, oeURL)
        } catch (e: Exception) {
            log.error { "Couldn't activate eregAPI - ${e.message}" }
        } finally {
            srv.stop()
        }
    }
}
