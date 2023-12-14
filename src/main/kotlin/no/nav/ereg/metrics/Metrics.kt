package no.nav.ereg.metrics

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.hotspot.DefaultExports
import mu.KotlinLogging

object Metrics {
    private val log = KotlinLogging.logger { }
    val cRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

    val blockedByFilter: Gauge = registerGauge("blocked_by_filter")

    fun registerGauge(name: String): Gauge {
        return Gauge.build().name(name).help(name).register()
    }
    fun registerLabelGauge(name: String, label: String): Gauge {
        return Gauge.build().name(name).help(name).labelNames(label).register()
    }
    init {
        DefaultExports.initialize()
        log.info { "Prometheus metrics are ready" }
    }
}

data class KConsumerMetrics(
    val consumerLatency: Histogram = Histogram
        .build()
        .name("kafka_consumer_latency_seconds_histogram")
        .help("Kafka consumer round trip (latency) since last restart")
        .register()
)

// some metrics for Salesforce client
data class SFMetrics(
    val responseLatency: Histogram = Histogram
        .build()
        .name("sf_response_latency_seconds_histogram")
        .help("Salesforce response latency since last restart")
        .register(),
    val failedAccessTokenRequest: Gauge = Gauge
        .build()
        .name("sf_failed_access_token_request_gauge")
        .help("No. of failed access token requests to Salesforce since last restart")
        .register(),
    val postRequest: Gauge = Gauge
        .build()
        .name("sf_post_request_gauge")
        .help("No. of post requests to Salesforce since last restart")
        .register(),
    val accessTokenRefresh: Gauge = Gauge
        .build()
        .name("sf_access_token_refresh_gauge")
        .help("No. of required access token refresh to Salesforce since last restart")
        .register()
) {
    fun clear() {
        failedAccessTokenRequest.clear()
        postRequest.clear()
        accessTokenRefresh.clear()
    }
}

data class KCommonMetrics(
    val pollErrorAuthentication: Gauge = Metrics.registerGauge("pollErrorAuthentication"),
    val pollErrorAuthorization: Gauge = Metrics.registerGauge("pollErrorAuthorization"),
    val pollErrorDeserialization: Gauge = Metrics.registerGauge("pollErrorDeserialization"),
    val commitErrorTimeBetweenPolls: Gauge = Metrics.registerGauge("commitErrorTimeBetweenPolls"),
    val consumeErrorServiceUnavailable: Gauge = Metrics.registerGauge("consumeErrorServiceUnavailable"),
    val unknownErrorConsume: Gauge = Metrics.registerGauge("unknownErrorConsume"),
    val unknownErrorPoll: Gauge = Metrics.registerGauge("unknownErrorPoll"),
    val unknownErrorCommit: Gauge = Metrics.registerGauge("unknownErrorCommit"),
    val noOfConsumedEvents: Gauge = Metrics.registerGauge("kafka_consumed_event_gauge"),
    val noOfEventsBlockedByFilter: Gauge = Metrics.registerGauge("kafka_blocked_by_filter_gauge"),
    val noOfPostedEvents: Gauge = Metrics.registerGauge("sf_posted_event_gauge"),
    val producerIssues: Gauge = Metrics.registerGauge("producer_issues"),
    val consumerIssues: Gauge = Metrics.registerGauge("consumer_issues"),
    val latestPostedOffset: Gauge = Metrics.registerLabelGauge("latest_posted_offset", "partition"),
    val latestConsumedOffset: Gauge = Metrics.registerLabelGauge("latest_consumed_offset", "partition")
)

fun KCommonMetrics.clearWorkSessionMetrics() {
    noOfConsumedEvents.clear()
    noOfPostedEvents.clear()
}

val kCommonMetrics = KCommonMetrics()
var kErrorState = ErrorState.NONE
var currentConsumerMessageHost = "DEFAULT"
var kafkaConsumerOffsetRangeBoard: MutableMap<String, Pair<Long, Long>> = mutableMapOf()
var numberOfWorkSessionsWithoutEvents = 0

const val POSTFIX_FAIL = "-FAIL"
const val POSTFIX_FIRST = "-FIRST"
const val POSTFIX_LATEST = "-LATEST"

enum class ErrorState() {
    NONE, UNKNOWN_ERROR, AUTHORIZATION, AUTHENTICATION, DESERIALIZATION, TIME_BETWEEN_POLLS,
    SERVICE_UNAVAILABLE, TOPIC_ASSIGNMENT
}
