package no.nav.ereg

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging

object Metrics {
    private val log = KotlinLogging.logger { }

    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    val receivedBytes: Histogram =
        Histogram
            .build()
            .name("response_size_bytes_histogram")
            .labelNames("type")
            .help("JSON stream in bytes")
            .register()

    val responseLatency: Histogram =
        Histogram
            .build()
            .name("response_latency_seconds_histogram")
            .labelNames("type")
            .help("JSON stream response latency")
            .register()

    val successfulRequest: Gauge =
        Gauge
            .build()
            .name("successful_request_gauge")
            .labelNames("type")
            .help("No. of successful requests since last restart")
            .register()

    val failedRequest: Gauge =
        Gauge
            .build()
            .name("failed_request_gauge")
            .labelNames("type")
            .help("No. of failed requests since last restart")
            .register()

    val noOfAttempts: Gauge =
        Gauge
            .build()
            .name("attempt_gauge")
            .help("No. of attempts since last successful run")
            .register()

    val publishedOrgs: Gauge =
        Gauge
            .build()
            .name("published_organisation_gauge")
            .labelNames("type", "status")
            .help("No. of organisations published to kafka in last work session")
            .register()

    val cachedOrgNoHashCode: Gauge =
        Gauge
            .build()
            .name("cached_orgno_hashcode_event_gauge")
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

    fun resetAll() { // Legacy, only used in test

        receivedBytes.clear()
        responseLatency.clear()
        successfulRequest.clear()
        failedRequest.clear()
        noOfAttempts.clear()
        publishedOrgs.clear()
        cachedOrgNoHashCode.clear()
    }
}
