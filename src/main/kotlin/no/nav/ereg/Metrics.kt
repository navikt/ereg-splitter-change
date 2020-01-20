package no.nav.ereg

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Summary
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging

object Metrics {

    private val log = KotlinLogging.logger { }

    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    val receivedBytes: Summary = Summary
        .build()
        .name("response_size_bytes")
        .labelNames("type")
        .help("JSON stream in bytes")
        .register()

    val responseLatency: Summary = Summary
        .build()
        .name("response_latency_seconds")
        .labelNames("type")
        .help("JSON stream response latency")
        .register()

    val successfulRequest: Counter = Counter
        .build()
        .name("successful_request_counter")
        .labelNames("type")
        .help("No. of successful requests since last restart")
        .register()

    val failedRequest: Counter = Counter
        .build()
        .name("failed_request_counter")
        .labelNames("type")
        .help("No. of failed requests since last restart")
        .register()

    val noOfAttempts: Counter = Counter
        .build()
        .name("attempt_counter")
        .help("No. of attempts since last successful run")
        .register()

    val publishedOrgs: Counter = Counter
        .build()
        .name("published_organisation_counter")
        .labelNames("type")
        .help("No. of organisations published to kafka in last work session")
        .register()

    val cachedOrgNoHashCode: Counter = Counter
        .build()
        .name("cached_orgno_hashcode_event_counter")
        .labelNames("type")
        .help("No. of cached orgno-hashcode consumed in last work session")
        .register()

    init {
        DefaultExports.initialize()
        log.info { "Prometheus metrics are ready" }
    }

    fun sessionReset() {
        publishedOrgs.clear()
        cachedOrgNoHashCode.clear()
    }

    fun resetAll() {
        receivedBytes.clear()
        responseLatency.clear()
        successfulRequest.clear()
        failedRequest.clear()
        noOfAttempts.clear()
        publishedOrgs.clear()
        cachedOrgNoHashCode.clear()
    }
}
