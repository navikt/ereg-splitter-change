package no.nav.ereg

import com.google.protobuf.InvalidProtocolBufferException
import mu.KotlinLogging
import no.nav.ereg.proto.EregOrganisationEventKey
import no.nav.ereg.proto.EregOrganisationEventValue
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

private val log = KotlinLogging.logger {}

fun getEnvOrDefault(env: String, defaultValue: String): String = System.getenv(env) ?: defaultValue

internal fun ByteArray.protobufSafeParseKey(): EregOrganisationEventKey = this.let { ba ->
    try {
        EregOrganisationEventKey.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        ServerState.state = ServerStates.ProtobufIssues
        log.error { "Failure when parsing protobuf for kafka key - ${e.message}" }
        EregOrganisationEventKey.getDefaultInstance()
    }
}

internal fun ByteArray.protobufSafeParseValue(): EregOrganisationEventValue = this.let { ba ->
    try {
        EregOrganisationEventValue.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        ServerState.state = ServerStates.ProtobufIssues
        log.error { "Failure when parsing protobuf for kafka value - ${e.message}" }
        EregOrganisationEventValue.getDefaultInstance()
    }
}

internal fun <K, V> KafkaProducer<K, V>.send(topic: String, key: K, value: V): Boolean = this.runCatching {
    send(ProducerRecord(topic, key, value)).get().hasOffset()
}
    .onFailure {
        ServerState.state = ServerStates.KafkaIssues
        log.error { "KafkaProducer failure when sending data to kafka - ${it.message}" }
    }
    .getOrDefault(false)

/**
 * KafkaPayload is a simple key-value to be sent to Kafka
 */
internal data class KafkaPayload<K, V> (
    val key: K,
    val value: V
)

/**
 * publishIterator - iterate and send each element to kafka, unless an something occur
 */
internal tailrec fun <K, V> KafkaProducer<K, V>.publishIterator(
    iter: Iterator<KafkaPayload<K, V>>,
    topic: String,
    sendIsOk: Boolean = true,
    noOfEvents: Int = 0
): Int =
    if (!(iter.hasNext() && sendIsOk)) if (!sendIsOk) noOfEvents - 1 else noOfEvents
    else publishIterator(
        iter,
        topic,
        iter.next().let {
            // send(topic, it.key, it.value)
            workMetrics.wouldHaveNumberOfPublishedOrgs.inc()
            true
        },
        noOfEvents + 1
    )
