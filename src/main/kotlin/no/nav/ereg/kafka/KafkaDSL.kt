package no.nav.ereg.kafka

import mu.KotlinLogging
import no.nav.ereg.Bootstrap.conditionalWait
import no.nav.ereg.env_KAFKA_BROKERS
import no.nav.ereg.env_KAFKA_CLIENTID
import no.nav.ereg.env_KAFKA_CREDSTORE_PASSWORD
import no.nav.ereg.env_KAFKA_KEYSTORE_PATH
import no.nav.ereg.env_KAFKA_POLL_DURATION
import no.nav.ereg.env_KAFKA_TOPIC
import no.nav.ereg.env_KAFKA_TRUSTSTORE_PATH
import no.nav.ereg.metrics.ErrorState
import no.nav.ereg.metrics.KConsumerMetrics
import no.nav.ereg.metrics.POSTFIX_FAIL
import no.nav.ereg.metrics.POSTFIX_FIRST
import no.nav.ereg.metrics.POSTFIX_LATEST
import no.nav.ereg.metrics.currentConsumerMessageHost
import no.nav.ereg.metrics.kCommonMetrics
import no.nav.ereg.metrics.kErrorState
import no.nav.ereg.metrics.kafkaConsumerOffsetRangeBoard
import no.nav.ereg.nais.PrestopHook
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties
import kotlin.Exception

private val log = KotlinLogging.logger {}

sealed class KafkaConsumerStates {
    object IsOk : KafkaConsumerStates()
    object IsOkNoCommit : KafkaConsumerStates()
    object HasIssues : KafkaConsumerStates()
    object IsFinished : KafkaConsumerStates()
}

fun env(env: String) = System.getenv(env)
fun envAsLong(env: String) = System.getenv(env).toLong()
/**
 * AKafkaConsumer
 * A class on top of the native Apache Kafka client
 * It provides a generic function: consume(handlePolledBatchOfRecords: (ConsumerRecords<K, V>) -> KafkaConsumerStates): Boolean
 * where one can insert code for what to do given a batch of polled records and report the result of that operation
 * with an instance of KafkaConsumerStates. (See usage in KafkaToSFPoster)
 * This class performs the polling cycle and handles metrics and logging.
 * The first poll gets extra retries due to connectivity latency to clusters when the app is initially booting up
 **/
open class AKafkaConsumer<K, V>(
    val config: Map<String, Any>,
    val topic: String = env(env_KAFKA_TOPIC),
    val pollDuration: Long = envAsLong(env_KAFKA_POLL_DURATION),
    val fromBeginning: Boolean = false,
    val hasCompletedAWorkSession: Boolean = false
) {
    companion object {
        val metrics = KConsumerMetrics()

        val configBase: Map<String, Any>
            get() = mapOf<String, Any>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to env(env_KAFKA_BROKERS),
                ConsumerConfig.GROUP_ID_CONFIG to env(env_KAFKA_CLIENTID),
                ConsumerConfig.CLIENT_ID_CONFIG to env(env_KAFKA_CLIENTID),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 200,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
                SaslConfigs.SASL_MECHANISM to "PLAIN",
                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to env(env_KAFKA_KEYSTORE_PATH),
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to env(env_KAFKA_CREDSTORE_PASSWORD),
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to env(env_KAFKA_TRUSTSTORE_PATH),
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to env(env_KAFKA_CREDSTORE_PASSWORD)
            )

        val configPlain: Map<String, Any>
            get() = configBase + mapOf<String, Any>(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
            )
    }

    internal fun <K, V> consume(
        config: Map<String, Any>,
        topic: String,
        pollDuration: Long = envAsLong(env_KAFKA_POLL_DURATION),
        fromBeginning: Boolean = false,
        hasCompletedAWorkSession: Boolean = false,
        doConsume: (ConsumerRecords<K, V>) -> KafkaConsumerStates
    ): Boolean =
        try {
            kErrorState = ErrorState.NONE
            KafkaConsumer<K, V>(Properties().apply { config.forEach { set(it.key, it.value) } })
                .apply {
                    if (fromBeginning)
                        this.runCatching {
                            assign(partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) })
                        }.onFailure {
                            kErrorState = ErrorState.TOPIC_ASSIGNMENT
                            log.error { "Failure during topic partition(s) assignment for $topic - ${it.message}" }
                        }
                    else
                        this.runCatching {
                            subscribe(listOf(topic))
                        }.onFailure {
                            kErrorState = ErrorState.TOPIC_ASSIGNMENT
                            log.error { "Failure during subscription for $topic -  ${it.message}" }
                        }
                }
                .use { c ->
                    if (fromBeginning) c.runCatching {
                        c.seekToBeginning(emptyList())
                    }.onFailure {
                        log.error { "Failure during SeekToBeginning - ${it.message}" }
                    }

                    var exitOk = true

                    tailrec fun loop(keepGoing: Boolean, retriesLeft: Int = 5): Unit = when {
                        PrestopHook.isActive() || !keepGoing -> (if (PrestopHook.isActive()) { log.warn { "Kafka stopped consuming prematurely due to hook" }; Unit } else Unit)
                        else -> {
                            val pollstate = c.pollAndConsumption(pollDuration, retriesLeft > 0, doConsume)
                            val retries = if (pollstate == Pollstate.RETRY) (retriesLeft - 1).coerceAtLeast(0) else 0
                            if (pollstate == Pollstate.RETRY) {
                                // We will retry poll in a minute
                                log.info { "Kafka consumer - No records found on $topic, retry consumption in 60s. Retries left: $retries" }
                                conditionalWait(60000)
                            } else if (pollstate == Pollstate.FAILURE) {
                                exitOk = false
                            }
                            loop(pollstate.shouldContinue(), retries)
                        }
                    }
                    log.info { "Kafka consumer is ready to consume from topic $topic" }
                    loop(true, if (hasCompletedAWorkSession) 0 else 5)
                    log.info { "Closing KafkaConsumer, kErrorstate: $kErrorState" }
                    return exitOk
                }
        } catch (e: Exception) {
            log.error { "Failure during kafka consumer construction - ${e.message}" }
            false
        }

    enum class Pollstate {
        FAILURE, RETRY, OK, FINISHED
    }

    fun Pollstate.shouldContinue(): Boolean {
        return this == Pollstate.RETRY || this == Pollstate.OK
    }

    private fun <K, V> KafkaConsumer<K, V>.pollAndConsumption(pollDuration: Long, retryIfNoRecords: Boolean, doConsume: (ConsumerRecords<K, V>) -> KafkaConsumerStates): Pollstate =
        runCatching {
            poll(Duration.ofMillis(pollDuration)) as ConsumerRecords<K, V>
        }
            .onFailure {
                log.error { "Failure during poll - ${it.message}, MsgHost: $currentConsumerMessageHost, Exception class name ${it.javaClass.name}\nMsgBoard: $kafkaConsumerOffsetRangeBoard \nGiven up on poll directly. Do not commit. Do not continue" }
                if (it is org.apache.kafka.common.errors.AuthorizationException) {
                    log.error { "Detected authorization exception (OZ). Should perform refresh" } // might be due to rotation of kafka certificates
                    kCommonMetrics.pollErrorAuthorization.inc()
                    kErrorState = ErrorState.AUTHORIZATION
                } else if (it is org.apache.kafka.common.errors.AuthenticationException) {
                    log.error { "Detected authentication exception (EC). Should perform refresh" } // might be due to rotation of kafka certificates
                    kCommonMetrics.pollErrorAuthentication.inc()
                    kErrorState = ErrorState.AUTHENTICATION
                } else if (it.message?.contains("deserializing") == true) {
                    log.error { "Detected deserializing exception." }
                    kCommonMetrics.pollErrorDeserialization.inc()
                    kErrorState = ErrorState.DESERIALIZATION
                } else {
                    log.error { "Unknown/unhandled error at poll" }
                    kCommonMetrics.unknownErrorPoll.inc()
                    kErrorState = ErrorState.UNKNOWN_ERROR
                }
                return Pollstate.FAILURE
            }
            .getOrDefault(ConsumerRecords<K, V>(emptyMap()))
            .let { cRecords ->
                if (cRecords.isEmpty && retryIfNoRecords) {
                    return Pollstate.RETRY
                }
                val consumerState = AKafkaConsumer.metrics.consumerLatency.startTimer().let { rt ->
                    runCatching { doConsume(cRecords) }
                        .onFailure {
                            log.error { "Failure during doConsume, MsgHost: $currentConsumerMessageHost - cause: ${it.cause}, message: ${it.message}. Stack: ${it.printStackTrace()}, Exception class name ${it.javaClass.name}\\" }
                            if (it.message?.contains("failed to respond") == true || it.message?.contains("terminated the handshake") == true) {
                                kErrorState = ErrorState.SERVICE_UNAVAILABLE
                                kCommonMetrics.consumeErrorServiceUnavailable.inc()
                            } else {
                                kErrorState = ErrorState.UNKNOWN_ERROR
                                kCommonMetrics.unknownErrorConsume.inc()
                            }
                        }
                        .getOrDefault(KafkaConsumerStates.HasIssues).also { rt.observeDuration() }
                }
                when (consumerState) {
                    KafkaConsumerStates.IsOk -> {
                        try {
                            val hasConsumed = cRecords.count() > 0
                            if (hasConsumed) { // Only need to commit anything if records was fetched
                                commitSync()
                                if (!kafkaConsumerOffsetRangeBoard.containsKey("$currentConsumerMessageHost$POSTFIX_FIRST")) kafkaConsumerOffsetRangeBoard["$currentConsumerMessageHost$POSTFIX_FIRST"] = Pair(cRecords.first().offset(), cRecords.last().offset())
                                kafkaConsumerOffsetRangeBoard["$currentConsumerMessageHost$POSTFIX_LATEST"] = Pair(cRecords.first().offset(), cRecords.last().offset())
                            }
                            Pollstate.OK
                        } catch (e: Exception) {
                            if (cRecords.count() > 0) {
                                kafkaConsumerOffsetRangeBoard["$currentConsumerMessageHost$POSTFIX_FAIL"] = Pair(cRecords.first().offset(), cRecords.last().offset())
                            }
                            log.error {
                                "Failure during commit, MsgHost: $currentConsumerMessageHost, leaving - ${e.message}, Exception name: ${e.javaClass.name}\n" +
                                    "MsgBoard: $kafkaConsumerOffsetRangeBoard"
                            }
                            if (e.message?.contains("rebalanced") == true) {
                                log.error { "Detected rebalance/time between polls exception." } // might be due to rotation of kafka certificates
                                kCommonMetrics.commitErrorTimeBetweenPolls.inc()
                                kErrorState = ErrorState.TIME_BETWEEN_POLLS
                            } else {
                                kCommonMetrics.unknownErrorCommit.inc()
                                kErrorState = ErrorState.UNKNOWN_ERROR
                            }
                            Pollstate.FAILURE
                        }
                    }
                    KafkaConsumerStates.IsOkNoCommit -> Pollstate.OK
                    KafkaConsumerStates.HasIssues -> {
                        if (cRecords.count() > 0) {
                            kafkaConsumerOffsetRangeBoard["$currentConsumerMessageHost$POSTFIX_FAIL"] = Pair(cRecords.first().offset(), cRecords.last().offset())
                        }
                        log.error { "Leaving consumer on HasIssues ErrorState: ${kErrorState.name} (specific error should be reported earlier), MsgHost: $currentConsumerMessageHost\nMsgBoard: $kafkaConsumerOffsetRangeBoard" }
                        Pollstate.FAILURE
                    }
                    KafkaConsumerStates.IsFinished -> {
                        log.info {
                            "Consumer finished, leaving\n" +
                                "MsgBoard: $kafkaConsumerOffsetRangeBoard"
                        }
                        Pollstate.FINISHED
                    }
                }
            }

    fun consume(handlePolledBatchOfRecords: (ConsumerRecords<K, V>) -> KafkaConsumerStates): Boolean =
        consume(config, topic, pollDuration, fromBeginning, hasCompletedAWorkSession, handlePolledBatchOfRecords)
}

sealed class Key<out K> {
    object Missing : Key<Nothing>()
    data class Exist<K>(val k: K) : Key<K>()
}

sealed class Value<out V> {
    object Missing : Value<Nothing>()
    data class Exist<V>(val v: V) : Value<V>()
}

sealed class KeyAndValue<out K, out V> {
    object Missing : KeyAndValue<Nothing, Nothing>()
    data class Exist<K, V>(val k: K, val v: V) : KeyAndValue<K, V>()
}

sealed class KeyAndTombstone<out K> {
    object Missing : KeyAndTombstone<Nothing>()
    data class Exist<K>(val k: K) : KeyAndTombstone<K>()
}

sealed class AllRecords<out K, out V> {
    object Interrupted : AllRecords<Nothing, Nothing>()
    object Failure : AllRecords<Nothing, Nothing>()
    data class Exist<K, V>(val records: List<Pair<Key<K>, Value<V>>>) : AllRecords<K, V>()

    fun isOk(): Boolean = this is Exist
    fun hasMissingKey(): Boolean = (this is Exist) && this.records.map { it.first }.contains(Key.Missing)
    fun hasMissingValue(): Boolean = (this is Exist) && this.records.map { it.second }.contains(Value.Missing)
    fun getKeysValues(): List<KeyAndValue.Exist<out K, out V>> = if (this is Exist)
        this.records.map {
            if (it.first is Key.Exist && it.second is Value.Exist)
                KeyAndValue.Exist((it.first as Key.Exist<K>).k, (it.second as Value.Exist<V>).v)
            else KeyAndValue.Missing
        }.filterIsInstance<KeyAndValue.Exist<K, V>>()
    else emptyList()
    fun getKeysTombstones(): List<KeyAndTombstone.Exist<out K>> = if (this is Exist)
        this.records.map {
            if (it.first is Key.Exist && it.second is Value.Missing)
                KeyAndTombstone.Exist((it.first as Key.Exist<K>).k)
            else KeyAndTombstone.Missing
        }.filterIsInstance<KeyAndTombstone.Exist<K>>()
    else emptyList()
}

fun <K, V> getAllRecords(
    config: Map<String, Any>,
    topics: List<String>
): AllRecords<K, V> =
    try {
        KafkaConsumer<K, V>(Properties().apply { config.forEach { set(it.key, it.value) } })
            .apply {
                this.runCatching {
                    assign(
                        topics.flatMap { topic ->
                            partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
                        }
                    )
                }.onFailure {
                    log.error { "Failure during topic partition(s) assignment for $topics - ${it.message}" }
                }
            }
            .use { c ->
                c.runCatching { seekToBeginning(emptyList()) }
                    .onFailure { log.error { "Failure during SeekToBeginning - ${it.message}" } }

                tailrec fun loop(records: List<Pair<Key<K>, Value<V>>>, retriesWhenEmpty: Int = 10): AllRecords<K, V> = when {
                    PrestopHook.isActive() -> AllRecords.Interrupted
                    else -> {
                        val cr = c.runCatching { Pair(true, poll(Duration.ofMillis(2_000)) as ConsumerRecords<K, V>) }
                            .onFailure { log.error { "Failure during poll, get AllRecords - ${it.localizedMessage}" } }
                            .getOrDefault(Pair(false, ConsumerRecords<K, V>(emptyMap())))

                        when {
                            !cr.first -> AllRecords.Failure
                            cr.second.isEmpty -> {
                                if (records.isEmpty() && cr.second.count() == 0) {
                                    if (retriesWhenEmpty > 0) {
                                        log.info { "Did not find any records will poll again (left $retriesWhenEmpty times)" }
                                        loop(emptyList(), retriesWhenEmpty - 1)
                                    } else {
                                        log.warn { "Cannot find any records, is topic empty?" }
                                        AllRecords.Exist(emptyList())
                                    }
                                } else {
                                    AllRecords.Exist(records)
                                }
                            }
                            else -> loop(
                                (
                                    records + cr.second.map {
                                        Pair(
                                            if (it.key() == null) Key.Missing else Key.Exist(it.key() as K),
                                            if (it.value() == null) Value.Missing else Value.Exist(it.value() as V)
                                        )
                                    }
                                    )
                            )
                        }
                    }
                }
                loop(emptyList()).also { log.info { "Closing KafkaConsumer" } }
            }
    } catch (e: Exception) {
        log.error { "Failure during kafka consumer construction - ${e.message}" }
        AllRecords.Failure
    }

class AKafkaProducer<K, V>(
    val config: Map<String, Any>
) {
    fun produce(doProduce: KafkaProducer<K, V>.() -> Unit): Boolean = getKafkaProducerByConfig(config, doProduce)

    companion object {

        val configBase: Map<String, Any>
            get() = mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to env(env_KAFKA_BROKERS),
                ProducerConfig.BATCH_SIZE_CONFIG to 1024 * 1024, // 1 MB batch size
                ProducerConfig.BUFFER_MEMORY_CONFIG to 512 * 1024 * 1024, // 512 MB buffer
                ProducerConfig.MAX_REQUEST_SIZE_CONFIG to 10 * 1024 * 1024, // 10 M
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to "SSL",
                SaslConfigs.SASL_MECHANISM to "PLAIN",
                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to env(env_KAFKA_KEYSTORE_PATH),
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to env(env_KAFKA_CREDSTORE_PASSWORD),
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to env(env_KAFKA_TRUSTSTORE_PATH),
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to env(env_KAFKA_CREDSTORE_PASSWORD)
            )

        val configPlain: Map<String, Any>
            get() = configBase + mapOf<String, Any>(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
    }
}

internal fun <K, V> getKafkaProducerByConfig(config: Map<String, Any>, doProduce: KafkaProducer<K, V>.() -> Unit): Boolean =
    try {
        KafkaProducer<K, V>(
            Properties().apply { config.forEach { set(it.key, it.value) } }
        ).use {
            it.runCatching { doProduce() }
                .onFailure { log.error { "Failure during doProduce - ${it.localizedMessage}" } }
        }
        true
    } catch (e: Exception) {
        log.error { "Failure during kafka producer construction - ${e.message}" }
        false
    }

fun <K, V> KafkaProducer<K, V>.send(topic: String, key: K, value: V): Boolean = this.runCatching {
    send(ProducerRecord(topic, key, value)).get().hasOffset()
}
    .getOrDefault(false)

fun <K, V> KafkaProducer<K, V>.sendNullKey(topic: String, value: V): Boolean = this.runCatching {
    send(ProducerRecord(topic, null, value)).get().hasOffset()
}
    .getOrDefault(false)

fun <K, V> KafkaProducer<K, V>.sendNullValue(topic: String, key: K): Boolean = this.runCatching {
    send(ProducerRecord(topic, key, null)).get().hasOffset()
}

    .getOrDefault(false)
