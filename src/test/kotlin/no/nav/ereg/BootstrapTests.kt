package no.nav.ereg

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer

class BootstrapTests : StringSpec() {

    // establish kafka environment
    private val topicNoAccess = "topic-no-access"
    private val topicAccess = "topic-access"
    private val kafkaUser = JAASCredential("srvkafkap1", "kafkap1")

    private val ke = KafkaEnvironment(
        topicNames = listOf(topicNoAccess, topicAccess),
        withSecurity = true,
        users = listOf(kafkaUser),
        autoStart = true
    )
        .addProducerToTopic(kafkaUser.username, topicAccess)
        .addConsumerToTopic(kafkaUser.username, topicAccess)

    init {

        "Bootstrap should retry according to config when no access to topic".config(enabled = false) {

            // same logic applies to kafka down, non authe. user - whatever kafka issue
            // excluded from tests due to time consumption

            eregAPI(9021) { ueURL, oeURL ->
                EnvVar(
                    kafkaBrokers = ke.brokersURL,
                    kafkaClientID = "TEST",
                    kafkaSecurity = "TRUE",
                    kafkaSecProt = "SASL_PLAINTEXT",
                    kafkaSaslMec = "PLAIN",
                    kafkaUser = kafkaUser.username,
                    kafkaPassword = kafkaUser.password,
                    kafkaTopic = topicNoAccess,
                    kafkaProducerTimeout = 30_000,

                    eregUEUrl = ueURL,
                    eregOEUrl = oeURL,

                    runEachMorning = "FALSE",
                    maxAttempts = 2,
                    msBetweenAttempts = 5_000
                ).also { ev ->
                    Bootstrap.start(ev)

                    ServerState.state shouldBe ServerStates.KafkaIssues
                    Metrics.noOfAttempts.get().toInt() shouldBe ev.maxAttempts
                }
            }
        }

        "Bootstrap should work when access to topic".config(enabled = false) {

            // see injection of events in beforeSpec

            eregAPI(9021, FILUENHETERCHANGED, FILOENHETERCHANGED) { ueURL, oeURL ->
                EnvVar(
                    kafkaBrokers = ke.brokersURL,
                    kafkaClientID = "TEST",
                    kafkaSecurity = "TRUE",
                    kafkaSecProt = "SASL_PLAINTEXT",
                    kafkaSaslMec = "PLAIN",
                    kafkaUser = kafkaUser.username,
                    kafkaPassword = kafkaUser.password,
                    kafkaTopic = topicAccess,
                    kafkaProducerTimeout = 30_000,

                    eregUEUrl = ueURL,
                    eregOEUrl = oeURL,

                    runEachMorning = "FALSE",
                    maxAttempts = -1,
                    msBetweenAttempts = 5_000
                ).also { ev ->
                    Bootstrap.start(ev)
                    // cached events read before producing to topic, thus 0

                    ServerState.state shouldBe ServerStates.IsOk
                    Metrics.noOfAttempts.get().toInt() shouldBe 0

                    Metrics.publishedOrgs.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 2
                    Metrics.successfulRequest.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 1
                    Metrics.failedRequest.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 0
                    Metrics.cachedOrgNoHashCode.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 5

                    Metrics.publishedOrgs.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 3
                    Metrics.successfulRequest.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 1
                    Metrics.failedRequest.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 0
                    Metrics.cachedOrgNoHashCode.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 5

                    Bootstrap.start(ev)
                    // cached events read before producing to topic, thus the previous 5 above
                    // no changes introduced - 0 published events

                    ServerState.state shouldBe ServerStates.IsOk
                    Metrics.noOfAttempts.get().toInt() shouldBe 0

                    Metrics.publishedOrgs.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 0
                    Metrics.successfulRequest.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 2
                    Metrics.failedRequest.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 0
                    // since this topic is non-compaction - will get changes added, thus 5 + 2
                    Metrics.cachedOrgNoHashCode.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 7

                    Metrics.publishedOrgs.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 0
                    Metrics.successfulRequest.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 2
                    Metrics.failedRequest.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 0
                    // since this topic is non-compaction - will get changes added, thus 5 + 3
                    Metrics.cachedOrgNoHashCode.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 8
                }
            }
        }

        "Bootstrap should retry according to config when no access to ereg".config(enabled = false) {

            // same logic applies to kafka down, non authe. user - whatever kafka issue
            // excluded from tests due to time consumption

            eregAPI(9021) { _, _ ->
                EnvVar(
                    kafkaBrokers = ke.brokersURL,
                    kafkaClientID = "TEST",
                    kafkaSecurity = "TRUE",
                    kafkaSecProt = "SASL_PLAINTEXT",
                    kafkaSaslMec = "PLAIN",
                    kafkaUser = kafkaUser.username,
                    kafkaPassword = kafkaUser.password,
                    kafkaTopic = topicAccess,
                    kafkaProducerTimeout = 30_000,

                    eregUEUrl = "invalid",
                    eregOEUrl = "invalid",

                    runEachMorning = "FALSE",
                    maxAttempts = 2,
                    msBetweenAttempts = 5_000
                ).also { ev ->
                    Bootstrap.start(ev)
                    // In case of failure for Enheter, will not try Underenheter...

                    ServerState.state shouldBe ServerStates.EregIssues
                    Metrics.noOfAttempts.get().toInt() shouldBe ev.maxAttempts

                    Metrics.successfulRequest.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe 0
                    Metrics.failedRequest.labels(EREGEntityType.ENHET.toString()).get().toInt() shouldBe ev.maxAttempts

                    Metrics.successfulRequest.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 0
                    Metrics.failedRequest.labels(EREGEntityType.UNDERENHET.toString()).get().toInt() shouldBe 0
                }
            }
        }
    }

    override fun beforeSpec(spec: Spec) {
        super.beforeSpec(spec)
        // need to inject some events in order to avoid cache issues
        eregAPI(9021) { ueURL, oeURL ->
            getKafkaProducerByConfig<ByteArray, ByteArray>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.CLIENT_ID_CONFIG to "TEST"
                ).addKafkaSecurity(kafkaUser.username, kafkaUser.password)
            ) {
                listOf(
                    EREGEntity(EREGEntityType.ENHET, oeURL, ""),
                    EREGEntity(EREGEntityType.UNDERENHET, ueURL, "")
                ).forEach { eregEntity ->
                    // only do the work if everything is ok so far
                    if (!ShutdownHook.isActive() && ServerState.isOk()) {
                        eregEntity.getJsonAsSequenceIterator(emptyMap()) { seqIter ->
                            publishIterator(seqIter, topicAccess)
                        } // end of use for InputStreamReader - AutoCloseable
                    }
                }
            }
        }
    }

    override fun beforeTest(testCase: TestCase) {
        super.beforeTest(testCase)
        Metrics.resetAll()
    }

    override fun afterSpec(spec: Spec) {
        super.afterSpec(spec)
        ke.tearDown()
    }
}
