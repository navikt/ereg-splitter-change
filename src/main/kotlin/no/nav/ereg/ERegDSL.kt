package no.nav.ereg

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import mu.KotlinLogging
import no.nav.ereg.proto.EregOrganisationEventKey
import no.nav.ereg.proto.EregOrganisationEventValue
import org.apache.http.HttpHost
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClients
import org.http4k.client.ApacheClient
import org.http4k.core.BodyMode
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import org.http4k.filter.gunzipped
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.StringBuilder
import java.net.URI

private val log = KotlinLogging.logger {}

const val EV_eregUEUrl = "EREG_UEURL"
const val EV_eregUEAccept = "EREG_UEACCEPT"
const val EV_eregOEUrl = "EREG_OEURL"
const val EV_eregOEAccept = "EREG_OEACCEPT"

const val EV_httpsProxy = "HTTPS_PROXY"

val eregUEUrl = getEnvOrDefault(EV_eregUEUrl, "")
val eregUEAccept = getEnvOrDefault(EV_eregUEAccept, "")
val eregOEUrl = getEnvOrDefault(EV_eregOEUrl, "")
val eregOEAccept = getEnvOrDefault(EV_eregOEAccept, "")

val httpsProxy: String = getEnvOrDefault(EV_httpsProxy, "")

data class EREGEntity(
    val type: EREGEntityType,
    val url: String,
    val acceptHeaderValue: String
)

enum class EREGEntityType {
    UNDERENHET,
    ENHET
}

var cacheFileStatusMap: MutableMap<String, FileStatus> = mutableMapOf()

private fun EREGEntity.getRequest() = Request(Method.GET, this.url).header("Accept", this.acceptHeaderValue)

internal fun EREGEntity.getJsonAsSequenceIterator(
    cache: Map<String, Int>,
    doConsume: (Iterator<KafkaPayload<ByteArray, ByteArray>>) -> Unit
): Boolean = this.let { eregEntity ->

    val responseTime = Metrics.responseLatency.labels(eregEntity.type.toString()).startTimer()

    log.info { "${eregEntity.type}, request json data set as stream" }

    val hp = httpsProxy
    val apacheHttpClient = if (hp.isNotEmpty()) {
        ApacheClient(
            client =
                HttpClients.custom()
                    .setDefaultRequestConfig(
                        RequestConfig.custom()
                            .setProxy(HttpHost(URI(hp).host, URI(hp).port, URI(hp).scheme))
                            .setRedirectsEnabled(false)
                            .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                            .build()
                    )
                    .build(),
            responseBodyMode = BodyMode.Stream
        )
    } else ApacheClient(responseBodyMode = BodyMode.Stream)

    val resp = apacheHttpClient
        .runCatching { invoke(eregEntity.getRequest()).also { responseTime.observeDuration() } }
        .onSuccess { response ->
            if (response.status.successful) {
                Metrics.successfulRequest.labels(eregEntity.type.toString()).inc()
                log.info { "${eregEntity.type}, successful response" }

                val streamAvailable = try {
                    Pair(
                        true,
                        response
                            .body
                            .gunzipped().also {
                                Metrics.receivedBytes.labels(eregEntity.type.toString()).observe(it.length?.toDouble() ?: 0.0)
                                log.info { "${eregEntity.type}, unzipped size is ${it.length} bytes" }
                            }
                            .stream
                    )
                } catch (e: Exception) {
                    ServerState.state = ServerStates.EregIssues
                    log.error { "${eregEntity.type}, failed when getting stream - ${e.message}" }
                    Pair(false, ByteArrayInputStream("".toByteArray(Charsets.UTF_8)))
                }
                if (streamAvailable.first)
                    streamAvailable.second
                        .use {
                            doConsume(it.asFilteredSequence(eregEntity.type, cache).iterator())
                            log.info { "${eregEntity.type}, consumption completed, closing StreamInputReader" }
                        }
            } else {
                Metrics.failedRequest.labels(eregEntity.type.toString()).inc()
                ServerState.state = ServerStates.EregIssues
                log.error { "${eregEntity.type}, failed response - ${response.status.code}. Attemped request: ${eregEntity.getRequest()}" }
            }
        }.onFailure {
            responseTime.observeDuration()
            Metrics.failedRequest.labels(eregEntity.type.toString()).inc()
            ServerState.state = ServerStates.EregIssues
            log.error { "${eregEntity.type}, failed when preparing for streaming - ${it.message}" }
        }
        .getOrDefault(Response(Status.EXPECTATION_FAILED).body(""))

    resp.status.successful // Not in use
}

internal sealed class ObjectInCacheStatus(val name: String) {
    object New : ObjectInCacheStatus("NY")
    object Updated : ObjectInCacheStatus("ENDRET")
    object NoChange : ObjectInCacheStatus("UENDRET")
}

internal fun Map<String, Int>.exists(jsonOrgObject: JsonOrgObject): ObjectInCacheStatus =
    if (!this.containsKey(jsonOrgObject.orgNo))
        ObjectInCacheStatus.New
    else if (this[jsonOrgObject.orgNo] != jsonOrgObject.hashCode)
        ObjectInCacheStatus.Updated.also {
            if (examples > 0) {
                examples--
                log.info { "EXAMPLE $examples. ORGNR: ${jsonOrgObject.orgNo} this: ${if (this[jsonOrgObject.orgNo] == 0) "0 (TOMBSTONE)" else this[jsonOrgObject.orgNo].toString()} do not match ${if (jsonOrgObject.hashCode == 0) "0 (TOMBSTONE)" else jsonOrgObject.hashCode.toString()}" }
            }
        }
    else
        ObjectInCacheStatus.NoChange

fun Int.toEvent(): String {
    if (this == 0) return "TOMBSTONE"
    return Int.toString()
}

/**
 * asSequence generates a lazy sequence of KafkaPayload mapped from JsonOrgObject as long as
 * - there are more data in stream
 * - there are no issues with the stream
 * - the org.no is found
 * - the org is new of updated
 */
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
        it.toKafkaPayload(eregType)
    }

/** JsonOrgObject is just a string representation of a json object in a json array
 * @property streamState is the status of the stream and thus the content
 * @property json is the json object, { ... }
 * @property orgNo is the org.no for the given json object
 * @property hashCode is the calculated hash code for value
 *
 * Compliance with interface KeyValue in order to publish to kafka as iterator from sequence based on InputStreamReader
 */

internal data class JsonOrgObject(
    val streamState: StreamState,
    val json: String = "",
    val orgNo: String = "",
    val hashCode: Int = 0
)

internal fun JsonOrgObject.isOk() = this.streamState == StreamState.STREAM_ONGOING && this.orgNo.isNotEmpty()

internal fun JsonOrgObject.addHashCode(): JsonOrgObject = this.json.hashCode().let { hashCode ->
    JsonOrgObject(
        this.streamState,
        this.json,
        this.orgNo,
        hashCode
    )
}

internal fun JsonOrgObject.toKafkaPayload(eregType: EREGEntityType): KafkaPayload<ByteArray, ByteArray> =
    KafkaPayload(
        key = EregOrganisationEventKey.newBuilder().let {
            it.orgNumber = this.orgNo
            it.orgType = when (eregType) {
                EREGEntityType.ENHET -> EregOrganisationEventKey.OrgType.ENHET
                EREGEntityType.UNDERENHET -> EregOrganisationEventKey.OrgType.UNDERENHET
            }
            it.build()
        }.toByteArray(),
        value = EregOrganisationEventValue.newBuilder().let {
            it.orgAsJson = this.json
            it.jsonHashCode = this.hashCode
            it.build()
        }.toByteArray()
    )

internal fun orgNumberAsKey(orgNumber: String): ByteArray =
    EregOrganisationEventKey.newBuilder().apply {
        this.orgNumber = orgNumber
        orgType = EregOrganisationEventKey.OrgType.ENHET
    }.build().toByteArray()

internal enum class StreamState(val value: Int) {
    STREAM_EXCEPTION(-2),
    STREAM_ONGOING(0),
    STREAM_FINISHED(-1) // according to InputStream.read()
}

private fun InputStreamReader.safeRead(): Int =
    this.runCatching { read() }
        .onFailure {
            ServerState.state = ServerStates.EregIssues
            log.error { "Stream reading error - ${it.message}" }
        }
        .getOrDefault(StreamState.STREAM_EXCEPTION.value)

/**
 * captureOrgNo gets recursively the org. no from the current JsonOrgObject in stream
 * Focusing on the json fragment after ':'
 * Fragment example
 * "organisasjonsnummer" : "981452607",
 *
 */
internal tailrec fun InputStreamReader.captureOrgNo(
    org: StringBuilder,
    orgNo: StringBuilder = StringBuilder()
): Pair<StringBuilder, String> = when (val i = safeRead()) {

    // Space
    32 -> captureOrgNo(org.append(i.toChar()), orgNo)
    // "
    34 -> {
        // the last " and completed orgNo
        if (orgNo.isNotEmpty()) Pair(org.append(i.toChar()), orgNo.toString())
        // the 1st "
        else captureOrgNo(org.append(i.toChar()), orgNo)
    }
    // in between 1st and last "
    else -> captureOrgNo(org.append(i.toChar()), orgNo.append(i.toChar()))
}

/**
 * captureJsonOrgObject captures recursively a JsonOrgObject from the json array in stream
 * Concept example
 * [
 *  {
 *  ...
 *  }
 *  ...
 * ]
 *
 * It doesn't matter whether the json object has sub objects
 */
internal tailrec fun InputStreamReader.captureJsonOrgObject(
    balanceCP: Int = 0,
    org: StringBuilder = StringBuilder(),
    orgNo: String = ""
): JsonOrgObject = when (val i = safeRead()) {

    -1 -> {
        log.info { "Stream completed successfully!" }
        JsonOrgObject(StreamState.STREAM_FINISHED)
    }
    -2 -> {
        log.warn { "Stream processing failure" }
        JsonOrgObject(StreamState.STREAM_EXCEPTION)
    }
    // : for getting the org no directly from the stream
    58 -> {
        // Prerequisite - assuming org no as key-value at top level in json org object
        val oNo1 = "\"organisasjonsnummer\""
        val oNo2 = "\"organisasjonsnummer\" "
        if (balanceCP == 1 && orgNo.isEmpty() && (org.endsWith(oNo1) || org.endsWith(oNo2))
        ) {
            val p = captureOrgNo(org.append(i.toChar()))
            captureJsonOrgObject(balanceCP, p.first, p.second)
        } else captureJsonOrgObject(balanceCP, org.append(i.toChar()), orgNo)
    }
    // { is either start of a json object in json array, or a sub object inside json object
    123 -> captureJsonOrgObject(balanceCP + 1, org.append(i.toChar()), orgNo)
    // }
    125 ->
        // the } completing the json object in json array
        if (balanceCP - 1 == 0) {
            val result = org.append(i.toChar()).toString()
            JsonOrgObject(StreamState.STREAM_ONGOING, result, orgNo).addHashCode()
        }
        // otherwise, just continue in the not completed json object
        else captureJsonOrgObject(balanceCP - 1, org.append(i.toChar()), orgNo)
    else ->
        // capture data when being inside json object
        if (balanceCP >= 1) captureJsonOrgObject(balanceCP, org.append(i.toChar()), orgNo)
        // skip data outside json object, e.g. [ ]
        else captureJsonOrgObject(balanceCP, org, orgNo)
}

// This function parses a JSON array stream and returns a Sequence of JsonObjects.
fun InputStream.asJsonObjectSequence(): Sequence<JsonObject> = sequence {
    val reader = JsonReader(InputStreamReader(this@asJsonObjectSequence))

    try {
        reader.beginArray() // Start of the JSON array
        while (reader.hasNext()) {
            // Read the next JSON object in the array
            val jsonObject = reader.readJsonObject()
            yield(jsonObject) // Yield the object into the sequence
        }
        reader.endArray() // End of the JSON array
    } catch (e: IOException) {
        // Handle any IO errors (e.g., file reading errors)
        println("Error reading JSON stream: ${e.message}")
    } finally {
        reader.close() // Close the reader to release resources
    }
}

// Extension function to read a single JsonObject from the stream
private fun JsonReader.readJsonObject(): JsonObject {
    val jsonObject = JsonObject()
    this.beginObject() // Begin reading the JSON object
    while (this.hasNext()) {
        val name = this.nextName() // Get the field name
        val value = this.nextValue() // Get the value for the field
        jsonObject.add(name, value) // Add it to the JsonObject
    }
    this.endObject() // End reading the JSON object
    return jsonObject
}

// Helper function to read the next value depending on the token type
private fun JsonReader.nextValue(): JsonElement {
    return when (this.peek()) {
        JsonToken.BEGIN_OBJECT -> this.readJsonObject()
        JsonToken.BEGIN_ARRAY -> this.readJsonArray()
        JsonToken.STRING -> JsonPrimitive(this.nextString())
        JsonToken.NUMBER -> JsonPrimitive(this.nextDouble())
        JsonToken.BOOLEAN -> JsonPrimitive(this.nextBoolean())
        JsonToken.NULL -> JsonNull.INSTANCE
        else -> throw IllegalStateException("Unexpected token: ${this.peek()}")
    }
}

// Extension function to handle reading an array when necessary
private fun JsonReader.readJsonArray(): JsonElement {
    val jsonArray = JsonArray()
    this.beginArray()
    while (this.hasNext()) {
        jsonArray.add(this.nextValue()) // Add elements to the array
    }
    this.endArray()
    return jsonArray
}
