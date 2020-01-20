package no.nav.ereg

import io.kotlintest.Spec
import io.kotlintest.TestCase
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import no.nav.common.JAASCredential
import no.nav.common.KafkaEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

class KafkaDSLTests : StringSpec() {

    private val topicString = "topicString"
    private val topicByteArray = "topicByteArray"
    private val kuP1 = JAASCredential("srvkafkap1", "kafkap1")
    private val kuC1 = JAASCredential("srvkafkac1", "kafkac1")
    private val ke = KafkaEnvironment(
        topicNames = listOf(topicString, topicByteArray),
        withSecurity = true,
        users = listOf(kuP1),
        autoStart = true
    )
        .addProducerToTopic(kuP1.username, topicString)
        .addProducerToTopic(kuP1.username, topicByteArray)
        .addConsumerToTopic(kuC1.username, topicString)
        .addConsumerToTopic(kuC1.username, topicByteArray)

    private val dataSet = setOf("event1", "event2", "event3")

    init {

        "KafkaDSL should not invoke doProduce when invalid config" {

            getKafkaProducerByConfig<String, String>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
                    // missing some config
                )
            ) {
                // no invocation
                true shouldBe false
            } shouldBe false
        }

        "KafkaDSL should not invoke doConsume when invalid config" {

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
                    // missing some config
                ),
                listOf(topicString)
            ) {
                // no invocation
                true shouldBe false
                ConsumerStates.IsFinished
            } shouldBe false
        }

        "KafkaDSL should invoke doProduce for non authe/autho user" {

            getKafkaProducerByConfig<String, String>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.CLIENT_ID_CONFIG to "TEST"
                ).addKafkaSecurity("invalidUser", "invalidPwd")
            ) {
                dataSet
                    .fold(true) { acc, s -> acc && send(topicString, "", s) } shouldBe false
            } shouldBe true
        }

        "KafkaDSL should invoke doConsume for non authe/autho user" {

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to "TEST",
                    ConsumerConfig.CLIENT_ID_CONFIG to "TEST",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).addKafkaSecurity("invalidUser", "invalidPwd"),
                listOf(topicString)
            ) { cRecords ->
                cRecords.fold(true) { acc, r -> acc && dataSet.contains(r.value()) } shouldBe false
                if (!cRecords.isEmpty) ConsumerStates.IsOkNoCommit else ConsumerStates.IsFinished
            } shouldBe true
        }

        "KafkaDSL should be able to produce events to topic" {

            getKafkaProducerByConfig<String, String>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.CLIENT_ID_CONFIG to "TEST"
                ).addKafkaSecurity(kuP1.username, kuP1.password)
            ) {
                listOf("event1", "event2", "event3")
                    .fold(true) { acc, s -> acc && send(topicString, "", s) } shouldBe true
            } shouldBe true
        }

        "KafkaDSL should be able to consume events from topic" {

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to "TEST",
                    ConsumerConfig.CLIENT_ID_CONFIG to "TEST",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).addKafkaSecurity(kuC1.username, kuC1.password),
                listOf(topicString)
            ) { cRecords ->
                cRecords.fold(true) { acc, r -> acc && dataSet.contains(r.value()) } shouldBe true
                if (!cRecords.isEmpty) ConsumerStates.IsOkNoCommit else ConsumerStates.IsFinished
            } shouldBe true
        }

        "KafkaDSL should be able to re-consume events from topic" {

            var noOfRecs = 0

            getKafkaConsumerByConfig<String, String>(
                mapOf(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    ConsumerConfig.GROUP_ID_CONFIG to "TEST",
                    ConsumerConfig.CLIENT_ID_CONFIG to "TEST",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
                ).addKafkaSecurity(kuC1.username, kuC1.password),
                listOf(topicString),
                fromBeginning = true
            ) { cRecords ->
                noOfRecs += cRecords.count()
                cRecords.fold(true) { acc, r -> acc && dataSet.contains(r.value()) } shouldBe true
                if (!cRecords.isEmpty) ConsumerStates.IsOkNoCommit else ConsumerStates.IsFinished
            } shouldBe true

            noOfRecs shouldBe dataSet.size
        }

        "KafkaDSL should be able to publish sequence iterator to topic for authen/autho user" {

            getKafkaProducerByConfig<ByteArray, ByteArray>(
                mapOf(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to ke.brokersURL,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.CLIENT_ID_CONFIG to "TEST"
                ).addKafkaSecurity(kuP1.username, kuP1.password)
            ) {

                eregAPI(9025) { ueURL, oeURL ->
                    EREGEntity(EREGEntityType.UNDERENHET, ueURL, "")
                        .getJsonAsSequenceIterator(emptyMap()) {
                                seqIter -> publishIterator(seqIter, topicByteArray) shouldBe 5
                        } shouldBe true

                    EREGEntity(EREGEntityType.ENHET, oeURL, "")
                        .getJsonAsSequenceIterator(emptyMap()) {
                                seqIter -> publishIterator(seqIter, topicByteArray) shouldBe 5
                        } shouldBe true
                }
            } shouldBe true
        }
    }

    override fun beforeTest(testCase: TestCase) {
        super.beforeTest(testCase)
        ServerState.reset()
        ShutdownHook.reset()
    }

    override fun afterSpec(spec: Spec) {
        super.afterSpec(spec)
        ke.tearDown()
    }
}
