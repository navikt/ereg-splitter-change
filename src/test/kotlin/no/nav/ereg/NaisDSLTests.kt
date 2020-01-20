package no.nav.ereg

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import org.http4k.client.ApacheClient
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Status

class NaisDSLTests : StringSpec({

    val endpoints = listOf(NaisDSL.ISALIVE, NaisDSL.ISREADY, NaisDSL.METRICS)

    "NaisDSL should give available Nais API at 8080 inside enable-context" {

        NaisDSL.enabled {
            endpoints.forEach { ep ->
                ApacheClient().invoke(Request(Method.GET, "http://localhost:8080$ep")).status shouldBe Status.OK
            }
        }
    }

    "NaisDSL should not give available Nais API at 8080 outside enable-context" {

        NaisDSL.enabled { }
        endpoints.forEach { ep ->
            ApacheClient().invoke(Request(Method.GET, "http://localhost:8080$ep")).status shouldNotBe Status.OK
        }
    }
})
