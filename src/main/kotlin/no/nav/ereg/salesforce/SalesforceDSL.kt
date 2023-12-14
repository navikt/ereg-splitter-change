package no.nav.ereg.salesforce

import com.google.gson.Gson
import io.prometheus.client.Histogram
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import no.nav.ereg.SALESFORCE_VERSION
import no.nav.ereg.SF_PATH_oAuth
import no.nav.ereg.env_HTTPS_PROXY
import no.nav.ereg.env_SF_TOKENHOST
import no.nav.ereg.kafka.env
import no.nav.ereg.metrics.ErrorState
import no.nav.ereg.metrics.SFMetrics
import no.nav.ereg.metrics.kCommonMetrics
import no.nav.ereg.metrics.kErrorState
import no.nav.ereg.secret_KeystorePassword
import no.nav.ereg.secret_PrivateKeyAlias
import no.nav.ereg.secret_PrivateKeyPassword
import no.nav.ereg.secret_SFClientID
import no.nav.ereg.secret_SFUsername
import no.nav.ereg.secret_keystoreJKSB64
import org.apache.http.HttpHost
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.HttpClients
import org.http4k.client.ApacheClient
import org.http4k.core.HttpHandler
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status
import java.io.File
import java.net.URI
import java.security.KeyStore
import java.security.PrivateKey

private val log = KotlinLogging.logger { }

val gson = Gson()

fun ApacheClient.supportProxy(httpsProxy: String): HttpHandler = httpsProxy.let { p ->
    when {
        p.isEmpty() -> this()
        else -> {
            val up = URI(p)
            this(
                client =
                    HttpClients.custom()
                        .setDefaultRequestConfig(
                            RequestConfig.custom()
                                .setProxy(HttpHost(up.host, up.port, up.scheme))
                                .setRedirectsEnabled(false)
                                .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                                .build()
                        )
                        .build()
            )
        }
    }
}

fun HttpHandler.measure(r: Request, m: Histogram): Response =
    m.startTimer().let { rt ->
        this(r).also {
            rt.observeDuration() // Histogram will store response time
            File("/tmp/lastTokenCall").writeText("uri: ${r.uri}, method: ${r.method}, body: ${r.body}, headers ${r.headers}")
        }
    }

fun ByteArray.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this)
fun String.encodeB64UrlSafe(): String = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this.toByteArray())
fun String.encodeB64(): String = org.apache.commons.codec.binary.Base64.encodeBase64String(this.toByteArray())
fun String.decodeB64(): ByteArray = org.apache.commons.codec.binary.Base64.decodeBase64(this)

/**
 * Getting access token from Salesforce is a little bit involving due to
 * - Need a private key from a key store where the public key is in the connected app in Salesforce
 * - Need to sign a claim (some facts about salesforce) with the private key
 * - Need an access token request using the signed claim
 */
sealed class KeystoreBase {
    object Missing : KeystoreBase()

    data class Exists(val privateKey: PrivateKey) : KeystoreBase() {
        fun sign(data: ByteArray): SignatureBase = runCatching {
            java.security.Signature.getInstance("SHA256withRSA")
                .apply {
                    initSign(privateKey)
                    update(data)
                }
                .run { SignatureBase.Exists(sign().encodeB64()) }
        }
            .onFailure { log.error { "Signing failed - ${it.localizedMessage}" } }
            .getOrDefault(SignatureBase.Missing)
    }

    fun signCheckIsOk(): Boolean = when (this) {
        is Missing -> false
        else -> ((this as Exists).sign("something".toByteArray())) != SignatureBase.Missing
    }

    companion object {
        fun fromBase64(ksB64: String, ksPwd: String, pkAlias: String, pkPwd: String): KeystoreBase = runCatching {
            Exists(
                KeyStore.getInstance("JKS")
                    .apply { load(ksB64.decodeB64().inputStream(), ksPwd.toCharArray()) }
                    .run { getKey(pkAlias, pkPwd.toCharArray()) as PrivateKey }
            )
        }
            .onFailure {
                log.error { "Keystore issues - ${it.localizedMessage}" }
            }
            .getOrDefault(Missing)
    }
}

sealed class SignatureBase {
    object Missing : SignatureBase()
    data class Exists(val content: String) : SignatureBase()
}

sealed class JWTClaimBase {
    object Missing : JWTClaimBase()

    data class Exists(
        val iss: String,
        val aud: String,
        val sub: String,
        val exp: String
    ) : JWTClaimBase() {

        private fun toJson(): String = gson.toJson(this)
        fun addHeader(): String = "${Header().toJson().encodeB64UrlSafe()}.${this.toJson().encodeB64UrlSafe()}"
    }

    companion object {
        fun fromJson(data: String): JWTClaimBase = runCatching {
            gson.fromJson(data, Exists::class.java)
        }
            .onFailure {
                log.error { "Parsing of JWTClaim failed" }
            }
            .getOrDefault(Missing)
    }

    // @Serializable
    data class Header(val alg: String = "RS256") {
        fun toJson(): String = gson.toJson(this)
    }
}

sealed class SFAccessToken {
    object Missing : SFAccessToken()

    // @Serializable
    data class Exists(
        val access_token: String = "",
        val scope: String = "",
        val instance_url: String = "",
        val id: String = "",
        val token_type: String = "",
        val issued_at: String = "",
        val signature: String = ""
    ) : SFAccessToken() {

        fun getPostRequest(sObjectPath: String): Request = log.debug { "Doing getPostRequest with instance_url: $instance_url sObjectPath: $sObjectPath" }.let {
            Request(
                Method.POST,
                "$instance_url$sObjectPath"
            )
                .header("Authorization", "$token_type $access_token")
                .header("Content-Type", "application/json;charset=UTF-8").also { log.debug { "Returning Request: $it" } }
        }
    }

    companion object {
        fun fromJson(data: String): SFAccessToken = runCatching { gson.fromJson(data, Exists::class.java) }
            .onFailure {
                log.error { "Parsing of authorization response failed - ${it.localizedMessage}" }
            }
            .getOrDefault(Missing)
    }
}

class SalesforceClient(
    private val httpClient: Lazy<HttpHandler> = lazy { ApacheClient.supportProxy(env(env_HTTPS_PROXY)) },
    private val tokenHost: Lazy<String> = lazy { env(env_SF_TOKENHOST) },
    private val clientID: String = env(secret_SFClientID),
    private val username: String = env(secret_SFUsername),
    private val keystore: KeystoreBase = KeystoreBase.fromBase64(
        env(secret_keystoreJKSB64),
        env(secret_KeystorePassword),
        env(secret_PrivateKeyAlias),
        env(secret_PrivateKeyPassword)
    ),
    private val retryDelay: Long = 1_500,
    transferAT: SFAccessToken = SFAccessToken.Missing
) {
    val SF_PATH_sObject = lazy { "/services/data/$SALESFORCE_VERSION/composite/sobjects" }

    private val claim: JWTClaimBase.Exists
        get() = JWTClaimBase.Exists(
            iss = clientID,
            aud = tokenHost.value,
            sub = username,
            exp = ((System.currentTimeMillis() / 1000) + 300).toString()
        )

    private val tokenURL: String
        get() = "${tokenHost.value}$SF_PATH_oAuth"

    private val accessTokenRequest: Request
        get() = claim.let { c ->

            // try to sign the complete claim (header included) using private key
            val fullClaimSignature = when (keystore) {
                is KeystoreBase.Missing -> SignatureBase.Missing
                else -> (keystore as KeystoreBase.Exists).sign(c.addHeader().toByteArray())
            }

            // try to get the signed content
            val content = when (fullClaimSignature) {
                is SignatureBase.Missing -> ""
                else -> (fullClaimSignature as SignatureBase.Exists).content
            }

            // build the request, the assertion to be verified by host with related public key
            Request(Method.POST, tokenURL)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .query("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
                .query("assertion", "${c.addHeader()}.$content")
        }

    private fun Response.parseAccessToken(): SFAccessToken = when (status) {
        Status.OK -> SFAccessToken.fromJson(bodyString())
        else -> {
            metrics.failedAccessTokenRequest.inc()
            // Report error only after last retry
            log.warn { "Failed access token request at parseAccessToken- ${status.description} ($status) "/*:  this.headers: ${this.headers} this: $this this.body: ${this.body}. Bodystring ${bodyString()}" */ }
            SFAccessToken.Missing
        }
    }

    // should do tailrec, but due to only a few iterations ...
    private fun getAccessTokenWithRetries(retry: Int = 1, maxRetries: Int = 4): SFAccessToken =
        httpClient.value.measure(accessTokenRequest, metrics.responseLatency).parseAccessToken().let {
            when (it) {
                is SFAccessToken.Missing -> {
                    if (retry > maxRetries) it.also { log.error { "Fail to fetch access token (including retries)" } }
                    else {
                        runCatching { runBlocking { delay(retry * retryDelay) } }
                        getAccessTokenWithRetries(retry + 1, maxRetries)
                    }
                }
                else -> (it as SFAccessToken.Exists)
            }
        }

    private var accessToken: SFAccessToken = transferAT

    fun enablesObjectPost(doSomething: ((String) -> Response) -> Unit): Boolean {

        val doPostRequest: (String, SFAccessToken.Exists) -> Response = { b, at ->
            httpClient.value.measure(at.getPostRequest(SF_PATH_sObject.value).body(b), metrics.responseLatency)
                .also { metrics.postRequest.inc() }
        }

        fun doPost(id: String, at: SFAccessToken.Exists): Pair<Response, SFAccessToken> =
            doPostRequest(id, at).let { r ->
                log.debug { "SF doPost initial response with http status - ${r.status} "/*and body ${r.body} and headers ${r.headers}" */ }
                when (r.status) {
                    Status.UNAUTHORIZED -> {
                        metrics.accessTokenRefresh.inc()
                        // try to get new access token
                        getAccessTokenWithRetries().let {
                            when (it) {
                                is SFAccessToken.Missing -> Pair(Response(Status.UNAUTHORIZED), it)
                                else -> Pair(doPostRequest(id, (it as SFAccessToken.Exists)), it)
                            }
                        }
                    }
                    Status.OK -> {
                        log.debug { "Returned status OK" }
                        Pair(r, at)
                    }
                    Status.CREATED -> {
                        log.info { "Returned status CREATED" }
                        Pair(r, at)
                    }
                    else -> {
                        log.error { "SF doPost issue with http status - ${r.status} "/*and body ${r.body} and headers ${r.headers}"*/ }
                        if (r.status.code == 503) {
                            kCommonMetrics.consumeErrorServiceUnavailable.inc()
                            kErrorState = ErrorState.SERVICE_UNAVAILABLE
                        } else {
                            kCommonMetrics.unknownErrorConsume.inc()
                            kErrorState = ErrorState.UNKNOWN_ERROR
                        }
                        Pair(r, at)
                    }
                }
            }

        // in case of missing access token from last invocation or very first start, try refresh
        if (accessToken is SFAccessToken.Missing) accessToken = getAccessTokenWithRetries()

        return when (accessToken) {
            is SFAccessToken.Missing -> false
            else -> {
                log.info { "SF access token exists" }
                val transfer: (String) -> Response = { b ->
                    doPost(b, (accessToken as SFAccessToken.Exists)).let { p ->
                        if ((accessToken as SFAccessToken.Exists) != p.second) accessToken = p.second
                        p.first
                    }
                }
                runCatching { doSomething(transfer) }
                    .onSuccess {
                        log.info { "Salesforce - end of sObject post availability with success" }
                    }
                    .onFailure {
                        log.error { "Salesforce - end of sObject post availability failed - ${it.message }" }
                    }
                true
            }
        }
    }

    companion object {
        val metrics = SFMetrics()
    }
}
