package no.nav.ereg

import io.prometheus.client.Gauge
import mu.KotlinLogging
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AllRecords
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.ShutdownHook
import no.nav.sf.library.getAllRecords
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

// Work environment dependencies
const val EV_kafkaTopic = "KAFKA_TOPIC"

val kafkaOrgTopic = AnEnvironment.getEnvOrDefault(EV_kafkaTopic, "$PROGNAME-producer")

data class WorkSettings(
    val kafkaConsumerOrg: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
    ),
    val kafkaProducerOrg: Map<String, Any> = AKafkaProducer.configBase + mapOf<String, Any>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java
    )
)

enum class FileStatus {
    NOT_PRESENT, SAME, UPDATED, NEW
}

sealed class Cache {
    object Missing : Cache()
    object Invalid : Cache()

    data class Exist(val map: Map<String, Int>) : Cache() {
        val isEmpty: Boolean
            get() = map.isEmpty()
        val statusBeforeFileRead: Map<String, FileStatus>
            get() = map.map { it.key to if (it.value == 0) FileStatus.SAME else FileStatus.NOT_PRESENT }.toMap()
    }

    companion object {
        fun load(kafkaConsumerConfig: Map<String, Any>, topic: String): Cache = kotlin.runCatching {
            when (val result = getAllRecords<ByteArray, ByteArray>(kafkaConsumerConfig, listOf(topic))) {
                is AllRecords.Exist -> {
                    when {
                        result.hasMissingKey() -> Missing
                                .also { log.error { "Cache has null in key" } }
                        // result.hasMissingValue() -> {
                        //    Missing
                        //        .also { log.error { "Cache has null in value" } }
                        // }
                        else -> {
                            val enheter: MutableMap<String, Int> = mutableMapOf()
                            val underenheter: MutableMap<String, Int> = mutableMapOf()
                            result.getKeysValues().map {
                                val key = it.k.protobufSafeParseKey()
                                Triple<String, EREGEntityType, Int>(
                                        key.orgNumber,
                                        EREGEntityType.valueOf(key.orgType.toString()),
                                        it.v.protobufSafeParseValue().jsonHashCode)
                            }
                                    .filter { it.first.isNotEmpty() }
                                    .groupBy { it.second }
                                    .let { tmp ->
                                        tmp[EREGEntityType.ENHET]?.let { list -> enheter.putAll(list.map { tri -> tri.first to tri.third }) }
                                        tmp[EREGEntityType.UNDERENHET]?.let { list -> underenheter.putAll(list.map { tri -> tri.first to tri.third }) }
                                    }
                            Metrics.cachedOrgNoHashCode.labels(EREGEntityType.ENHET.toString()).inc(enheter.size.toDouble())
                            Metrics.cachedOrgNoHashCode.labels(EREGEntityType.UNDERENHET.toString()).inc(underenheter.size.toDouble())
                            val tombstones: MutableSet<String> = mutableSetOf()
                            result.getKeysTombstones().map {
                                it.k.protobufSafeParseKey().orgNumber
                            }.filter { it.isNotEmpty() }.let { tombstones.addAll(it) }
                            val cacheMap: MutableMap<String, Int> = mutableMapOf()
                            cacheMap.putAll(enheter)
                            cacheMap.putAll(underenheter)
                            cacheMap.putAll(tombstones.map { it to 0 })

                            log.info { "Cache has ${enheter.size} ENHET - and ${underenheter.size} UNDERENHET entries - and ${tombstones.size} tombstones" }

                            Exist(cacheMap).also { log.info { "Cache size is ${it.map.size}" } }
                        }
                    }
                }
                else -> Missing
            }
        }
                .onFailure { log.error { "Error building Cache - ${it.message}" } }
                .getOrDefault(Invalid)
    }
}

sealed class ExitReason {
    object Issue : ExitReason()
    object NoEvents : ExitReason()
    object NoCache : ExitReason()
    object Work : ExitReason()

    fun isOK(): Boolean = this is Work || this is NoEvents
}

data class WMetrics(
    val sizeOfCache: Gauge = Gauge
            .build()
            .name("size_of_cache")
            .help("Size of ereg cache")
            .register(),
    val numberOfPublishedOrgs: Gauge = Gauge
        .build()
        .name("number_of_published_orgs")
        .help("Number of published orgs")
        .register(),

    val publishedTombstones: Gauge = Gauge
            .build()
            .name("published_tombstones")
            .help("Number of published tombstones")
            .register()
) {

    fun clearAll() {
        this.sizeOfCache.clear()
        this.numberOfPublishedOrgs.clear()
        this.publishedTombstones.clear()
    }
}

val workMetrics = WMetrics()

val ignoreCache = false
internal fun work(ws: WorkSettings): Pair<WorkSettings, ExitReason> {

    log.info { "bootstrap work session starting" }

    workMetrics.clearAll()
    ServerState.reset()

    val tmp = Cache.load(ws.kafkaConsumerOrg, kafkaOrgTopic)
    if (tmp is Cache.Missing) {
        log.error { "Could not read cache, leaving" }
        return Pair(ws, ExitReason.NoCache)
    }

    val cache = if (ignoreCache) Cache.Exist(mapOf()) else (tmp as Cache.Exist)

    workMetrics.sizeOfCache.set(cache.map.size.toDouble())

    cacheFileStatusMap.clear() // Make sure empty
    cacheFileStatusMap.putAll(cache.statusBeforeFileRead)

    log.info { "Continue work with Cache" }

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerOrg
    ).produce {
        listOf(
                EREGEntity(EREGEntityType.ENHET, eregOEUrl, eregOEAccept),
                EREGEntity(EREGEntityType.UNDERENHET, eregUEUrl, eregUEAccept)
        ).forEach { eregEntity ->
            // only do the work if everything is ok so far
            if (!ShutdownHook.isActive() && ServerState.isOk()) {
                eregEntity.getJsonAsSequenceIterator(cache.map) { seqIter ->
                    if (ServerState.isOk()) {
                        log.info { "${eregEntity.type}, got sequence iterator and server state ok, publishing changes to kafka" }
                        publishIterator(seqIter, kafkaOrgTopic)
                            .also { noOfEvents ->
                                log.info { "${eregEntity.type}, $noOfEvents orgs published to kafka ($kafkaOrgTopic)" }
                                workMetrics.numberOfPublishedOrgs.inc(noOfEvents.toDouble())
                            }
                    } else {
                        log.error { "Skipping ${eregEntity.type} due to server state issue ${ServerState.state.javaClass.name}" }
                    }
                } // end of use for InputStreamReader - AutoCloseable
            }
        }
        if (ServerState.isOk()) {
            cacheFileStatusMap.filter { it.value == FileStatus.NOT_PRESENT }.forEach {
                sendNullValue(kafkaOrgTopic, orgNumberAsKey(it.key)).let { sent ->
                    if (sent) {
                        workMetrics.publishedTombstones.inc()
                    } else {
                        log.error { "Issue when producing tombstone" }
                        ServerState.state = ServerStates.KafkaIssues
                    }
                }
            }
        } else {
            log.error { "Skipping tombstone publishing due to server state issue ${ServerState.state.javaClass.name}" }
        }

        log.info { "Published ${workMetrics.publishedTombstones.get().toInt()} tombstones. (Present tombstones in cache before: ${cache.map.count{it.value == 0}}" }
    }

    log.info {
        "Finished work session. Number of already existing: ${cacheFileStatusMap.values.count { it == FileStatus.SAME}}" +
                ", updated: ${cacheFileStatusMap.values.count { it == FileStatus.UPDATED }}" +
                ", new: ${cacheFileStatusMap.values.count { it == FileStatus.NEW }}" +
                ", not present (new tombstones): ${cacheFileStatusMap.values.count { it == FileStatus.NOT_PRESENT }}" +
                ", server state ok? ${ServerState.isOk()}"
    }

    cacheFileStatusMap.clear() // Free memory

    // TODO Legacy: Based on global ServerStates in publishiterator etc. Find another solution for error handling?
    return if (ServerState.isOk()) {
        Pair(ws, ExitReason.Work)
    } else {
        Pair(ws, ExitReason.Issue)
    }
}
