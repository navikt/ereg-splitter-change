package no.nav.ereg

import io.prometheus.client.Gauge
import mu.KotlinLogging
import no.nav.ereg.proto.EregOrganisationEventKey
import no.nav.sf.library.AKafkaConsumer
import no.nav.sf.library.AKafkaProducer
import no.nav.sf.library.AllRecords
import no.nav.sf.library.AnEnvironment
import no.nav.sf.library.KAFKA_LOCAL
import no.nav.sf.library.KafkaConsumerStates
import no.nav.sf.library.Key
import no.nav.sf.library.PROGNAME
import no.nav.sf.library.ShutdownHook
import no.nav.sf.library.Value
import no.nav.sf.library.getAllRecords
import no.nav.sf.library.sendNullValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

// Work environment dependencies
const val EV_kafkaTopic = "KAFKA_TOPIC"

val kafkaOrgTopic = AnEnvironment.getEnvOrDefault(EV_kafkaTopic, "$PROGNAME-producer")
val kafkaCacheTopicGcp = "team-dialog.ereg-cache" // same as above now

const val EV_kafkaKeystorePath = "KAFKA_KEYSTORE_PATH"
const val EV_kafkaCredstorePassword = "KAFKA_CREDSTORE_PASSWORD"
const val EV_kafkaTruststorePath = "KAFKA_TRUSTSTORE_PATH"
fun fetchEnv(env: String): String {
    return AnEnvironment.getEnvOrDefault(env, "$env missing")
}

data class WorkSettings(

    val kafkaConsumerOnPrem: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to AnEnvironment.getEnvOrDefault("KAFKA_BROKERS_ON_PREM", KAFKA_LOCAL)
    ),
    val kafkaProducerOnPrem: Map<String, Any> = AKafkaProducer.configBase + mapOf<String, Any>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to AnEnvironment.getEnvOrDefault("KAFKA_BROKERS_ON_PREM", KAFKA_LOCAL)
    ),
    val kafkaConsumerGcp: Map<String, Any> = AKafkaConsumer.configBase + mapOf<String, Any>(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            "security.protocol" to "SSL",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaKeystorePath),
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword),
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaTruststorePath),
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword)
    ),
    val kafkaProducerGcp: Map<String, Any> = AKafkaProducer.configBase + mapOf<String, Any>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            "security.protocol" to "SSL",
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaKeystorePath),
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword),
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG to fetchEnv(EV_kafkaTruststorePath),
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG to fetchEnv(EV_kafkaCredstorePassword)
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
            log.info { "Attempting load cache from topic $topic." }
            log.info { "Attempting load cache from config $kafkaConsumerConfig" }
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

                            var badCount = 0

                            val c = result.records.map {
                                if (it.first is Key.Exist && it.second is Value.Exist) {
                                    (it.first as Key.Exist).k.protobufSafeParseKey() to (it.second as Value.Exist).v.protobufSafeParseValue().jsonHashCode
                                } else if (it.first is Key.Exist && it.second is Value.Missing) {
                                    (it.first as Key.Exist).k.protobufSafeParseKey() to null
                                } else {
                                    badCount++
                                    null to null
                                }
                            }

                            val m = c.toMap()
                            log.info { " bad count $badCount" }
                            log.info { "C list length ${c.size}, m map size ${m.size}" }
                            /*
                            this.records.map {
            if (it.first is Key.Exist && it.second is Value.Missing)
                KeyAndTombstone.Exist((it.first as Key.Exist<K>).k)
            else KeyAndTombstone.Missing
        }.filterIsInstance<KeyAndTombstone.Exist<K>>()
                             */

                            val presenceUEinTombstones = underenheter.keys.filter { tombstones.contains(it) }.count()

                            val presenceEinUE = enheter.keys.filter { underenheter.contains(it) }.count()
                            log.info { "Presence of UE in tombstones $presenceUEinTombstones, E IN UE $presenceEinUE. Example UE: ${underenheter.keys.last()}" }

                            log.info { "Cache has ${enheter.size} ENHET - and ${underenheter.size} UNDERENHET entries - and ${tombstones.size} tombstones" }

                            log.info { "Otherwise derived caches has ${m.filter{it.key != null && it.value != null && it.key?.orgType == EregOrganisationEventKey.OrgType.ENHET}.count()} ENHET," +
                            " ${m.filter{it.key != null && it.value != null && it.key?.orgType == EregOrganisationEventKey.OrgType.UNDERENHET}.count()} UNDERENHET," +
                                    " ${m.filter{it.key != null && it.value == null && it.key?.orgType == EregOrganisationEventKey.OrgType.ENHET}.count()} TOMB ENHET," +
                                    " ${m.filter{it.key != null && it.value == null && it.key?.orgType == EregOrganisationEventKey.OrgType.UNDERENHET}.count()} TOMB UNDERENHET" +
                                    " ${m.filter{it.key != null && it.value == null}.count()} TOMBSTONES TOTAL"
                            }

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
var examples = 0

internal fun cacheToGcp(ws: WorkSettings) {
    log.info { "cache to gcp run starting" }

    var hasAccessedCache = false

    var cacheCount = 0
    var cacheCountTombstones = 0

    var publishCount = 0
    var publishCountTombstones = 0
    AKafkaProducer<ByteArray, ByteArray>(
        config = ws.kafkaProducerGcp
    ).produce {
        AKafkaConsumer<ByteArray, ByteArray?>(config = ws.kafkaConsumerOnPrem, fromBeginning = true).consume { cRecords ->
            if (cRecords.isEmpty()) {
                log.info { "CACHE Found no more records from cache - finished" }
                if (hasAccessedCache) {
                    KafkaConsumerStates.IsFinished
                } else {
                    log.info { "CACHE Expect to access records from cache will wait a minute and try again" }
                    Bootstrap.conditionalWait(60000)
                    KafkaConsumerStates.IsOk
                }
            } else {
                hasAccessedCache = true
                cacheCount += cRecords.count()
                log.info { "CACHE run consumed ${cRecords.count()}" }
                cRecords.forEach {
                    if (it.value() == null) {
                        cacheCountTombstones.inc()
                        if (sendNullValue(kafkaCacheTopicGcp, it.key())) { publishCountTombstones++ ; publishCount++ } else { log.error { "Fault in cache producer send tombstone" } }
                    } else {
                        if (send(kafkaCacheTopicGcp, it.key(), it.value()!!)) publishCount++ else { log.error { "Fault in cache producer send org" } }
                    }
                }
                KafkaConsumerStates.IsOk
            }
        }
    }
    log.info { "cache to gcp run finished - cacheCount $cacheCount (tombstones $cacheCountTombstones), publishCount $publishCount (tombstones $publishCountTombstones)" }
}

internal fun work(ws: WorkSettings): Pair<WorkSettings, ExitReason> {
    log.info { "bootstrap work session starting" }

    workMetrics.clearAll()
    ServerState.reset()

    val tmp = Cache.load(ws.kafkaConsumerGcp, kafkaOrgTopic)
    if (tmp is Cache.Missing) {
        log.error { "Could not read cache, leaving" }
        return Pair(ws, ExitReason.NoCache)
    }

    val cache = if (ignoreCache) Cache.Exist(mapOf()) else (tmp as Cache.Exist)

    workMetrics.sizeOfCache.set(cache.map.size.toDouble())

    cacheFileStatusMap.clear() // Make sure empty
    cacheFileStatusMap.putAll(cache.statusBeforeFileRead)

    // log.info { "SKIP work after cache -load Continue work with Cache" }
    // return Pair(ws, ExitReason.NoEvents)

    AKafkaProducer<ByteArray, ByteArray>(
            config = ws.kafkaProducerGcp
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
                workMetrics.publishedTombstones.inc()

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
