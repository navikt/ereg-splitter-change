package no.nav.ereg

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer

private val log = KotlinLogging.logger {}

internal fun getOrgNoHashCache(
    topic: String,
    ev: EnvVar
): MutableMap<String, Int> = mutableMapOf<String, Int>().let { map ->

    log.info { "Get orgno-hashcode cache from compaction log - ${ev.kafkaTopic}" }

    getKafkaConsumerByConfig<ByteArray, ByteArray>(
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ev.kafkaBrokers,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
            ConsumerConfig.GROUP_ID_CONFIG to ev.kafkaClientID,
            ConsumerConfig.CLIENT_ID_CONFIG to ev.kafkaClientID,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
        ).let { cMap ->
            if (ev.kafkaSecurityEnabled())
                cMap.addKafkaSecurity(ev.kafkaUser, ev.kafkaPassword, ev.kafkaSecProt, ev.kafkaSaslMec)
            else cMap
        },
        listOf(topic),
        fromBeginning = true
    ) { cRecords ->
        if (!cRecords.isEmpty) {
            map.putAll(
                cRecords
                    .map {
                        val key = it.key().protobufSafeParseKey()
                        Metrics.cachedOrgNoHashCode.labels(key.orgType.toString()).inc()
                        key.orgNumber to it.value().protobufSafeParseValue().jsonHashCode
                    }
                    .filter { it.first.isNotEmpty() }
            )
            ConsumerStates.IsOkNoCommit
        } else {
            log.info { "Cache completed - leaving kafka consumer loop" }
            ConsumerStates.IsFinished
        }
    }
    log.info { "Cache size for orgno-hashcode - ${map.size} entries" }

    map
}

internal fun work(ev: EnvVar) {

    log.info { "bootstrap work session starting" }

    val mapOrgNoHashCode = getOrgNoHashCache(ev.kafkaTopic, ev)
    if (ServerState.state == ServerStates.KafkaIssues && mapOrgNoHashCode.isEmpty()) {
        log.error { "Terminating work session since cache is empty due to kafka issues" }
        return
    }

    getKafkaProducerByConfig<ByteArray, ByteArray>(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to ev.kafkaBrokers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to ev.kafkaProducerTimeout,
            ProducerConfig.CLIENT_ID_CONFIG to ev.kafkaClientID
        ).let { map ->
            if (ev.kafkaSecurityEnabled())
                map.addKafkaSecurity(ev.kafkaUser, ev.kafkaPassword, ev.kafkaSecProt, ev.kafkaSaslMec)
            else map
        }
    ) {
        listOf(
            EREGEntity(EREGEntityType.ENHET, ev.eregOEUrl, ev.eregOEAccept),
            EREGEntity(EREGEntityType.UNDERENHET, ev.eregUEUrl, ev.eregUEAccept)
        ).forEach { eregEntity ->
            // only do the work if everything is ok so far
            if (!ShutdownHook.isActive() && ServerState.isOk()) {
                eregEntity.getJsonAsSequenceIterator(mapOrgNoHashCode) { seqIter ->
                    log.info { "${eregEntity.type}, got sequence iterator, publishing changes to kafka" }
                    publishIterator(seqIter, ev.kafkaTopic)
                        .also { noOfEvents ->
                            Metrics.publishedOrgs.labels(eregEntity.type.toString()).inc(noOfEvents.toDouble())
                            log.info { "${eregEntity.type}, $noOfEvents orgs published to kafka (${ev.kafkaTopic})" }
                        }
                } // end of use for InputStreamReader - AutoCloseable
            }
        }
    } // end of use for KafkaProducer - AutoCloseable
}
