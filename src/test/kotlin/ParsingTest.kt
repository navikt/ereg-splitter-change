import com.google.gson.GsonBuilder
import mu.KotlinLogging
import no.nav.ereg.EREGEntityType
import no.nav.ereg.FileStatus
import no.nav.ereg.JsonOrgObject
import no.nav.ereg.KafkaPayload
import no.nav.ereg.Metrics
import no.nav.ereg.ObjectInCacheStatus
import no.nav.ereg.ServerState
import no.nav.ereg.ServerStates
import no.nav.ereg.StreamState
import no.nav.ereg.addHashCode
import no.nav.ereg.asFilteredSequence
import no.nav.ereg.asJsonObjectSequence
import no.nav.ereg.cacheFileStatusMap
import no.nav.ereg.exists
import no.nav.ereg.isOk
import no.nav.ereg.kafkaOrgTopic
import no.nav.ereg.publishIterator
import no.nav.ereg.toKafkaPayload
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.filter.gunzipped
import java.io.ByteArrayInputStream
import java.io.InputStream

class ParsingTest {
    private val log = KotlinLogging.logger {}

    val gsonPretty = GsonBuilder().setPrettyPrinting().create()

    fun getFileAsByteArrayInputStream(filename: String): ByteArrayInputStream {
        val resource = this::class.java.classLoader.getResourceAsStream(filename)
            ?: throw IllegalArgumentException("File not found: $filename")
        return ByteArrayInputStream(resource.readAllBytes())
    }

    // @Test
    fun `test function with mocked apache client`() {
        val gzippedFileStream = getFileAsByteArrayInputStream("Scratch_enheter_alle.json.gz")

        log.info { "Got the stream" }
        // Create a mocked Response with the gzipped content as the stream
        val response = Response(Status.OK).body(gzippedFileStream, gzippedFileStream.available().toLong())

        val streamAvailable = try {
            Pair(
                true,
                response
                    .body.also { log.info { "bodyfeteched" } }
                    .gunzipped().also {
                        log.info { "Testing Enhet, unzipped size is ${it.length} bytes" }
                    }
                    .stream.also { log.info { "streamified" } }
            )
        } catch (e: Exception) {
            ServerState.state = ServerStates.EregIssues
            log.error { "Testing enhet, failed when getting stream - ${e.message}" }
            Pair(false, ByteArrayInputStream("".toByteArray(Charsets.UTF_8)))
        }

        fun InputStream.asFilteredSequence(
            eregType: EREGEntityType,
            cache: Map<String, Int>
        ): Sequence<KafkaPayload<ByteArray, ByteArray>> = this.asJsonObjectSequence().filter { it.has("organisasjonsnummer") }.map {
            JsonOrgObject(json = it.toString(), streamState = StreamState.STREAM_ONGOING, orgNo = it["organisasjonsnummer"].asString).addHashCode()
        }
            .filter { jsonOrgObject ->

                val status = cache.exists(jsonOrgObject)
                when (status) {
                    ObjectInCacheStatus.New -> cacheFileStatusMap[jsonOrgObject.orgNo] = FileStatus.NEW
                    ObjectInCacheStatus.Updated -> cacheFileStatusMap[jsonOrgObject.orgNo] = FileStatus.UPDATED
                    ObjectInCacheStatus.NoChange -> cacheFileStatusMap[jsonOrgObject.orgNo] = FileStatus.SAME
                }

                Metrics.publishedOrgs.labels(eregType.toString(), status.name).inc()
                status in listOf(ObjectInCacheStatus.New, ObjectInCacheStatus.Updated)
            }
            .map {
                println("object from stream: \n" + gsonPretty.toJson(it))
                it.toKafkaPayload(eregType)
            }

        fun publishIterator(
            iter: Iterator<KafkaPayload<ByteArray, ByteArray>>,
            topic: String
        ): Int {
            var sendIsOk = true
            var noOfEvents = 0

            log.info { "Ready to process - check hasNext" }

            try {
                log.info { "hasNExt - ${iter.hasNext()}" }
            } catch (t: Throwable) {
                log.error { t.stackTraceToString() }
            }

            while (iter.hasNext() && sendIsOk) {
                log.info { "hasNext and about to take 100, noOfEvents $noOfEvents" }
                val materializedBatch = iter.asSequence().take(100).toList()
                materializedBatch.forEach {
                    // Process payload here
                    // Example: send(topic, payload.key, payload.value)
                    sendIsOk = true
                    // workMetrics.wouldHaveNumberOfPublishedOrgs.inc()
                    noOfEvents++
                }
            }

            return noOfEvents
        }

        val doConsume: (Iterator<KafkaPayload<ByteArray, ByteArray>>) -> Unit = { seqIter ->
            if (ServerState.isOk()) {
                log.info { "testing enhet, testing got sequence iterator and server state ok, publishing changes to kafka" }

                publishIterator(seqIter, kafkaOrgTopic)
                    .also { noOfEvents ->
                        log.info { "Testing enhet, $noOfEvents orgs published to kafka ($kafkaOrgTopic)" }
                        // workMetrics.numberOfPublishedOrgs.inc(noOfEvents.toDouble())
                        // workMetrics.wouldHaveNumberOfPublishedOrgs.inc(noOfEvents.toDouble())
                    }
            } else {
                log.error { "Skipping testing enhet chunk due to server state issue ${ServerState.state.javaClass.name}" }
            }
        }

        if (streamAvailable.first) {

            log.info { "Should star working" }
            streamAvailable.second
                .use {
                    doConsume(it.asFilteredSequence(EREGEntityType.ENHET, mutableMapOf()).iterator())
                    log.info { "Testing enhet, consumption completed, closing StreamInputReader" }
                }

//            streamAvailable.second.asJsonObjectSequence().filter{ it.has("organisasjonsnummer") }.map {
//                JsonOrgObject(json = it.toString(), streamState = StreamState.STREAM_ONGOING, orgNo=it["organisasjonsnummer"].asString).addHashCode()
//            }.forEach { println( "easy way:\n${gsonPretty.toJson(it)}") }
        }
    }
}
