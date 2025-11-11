@file:Suppress("ktlint:standard:filename", "ktlint:standard:property-naming")

package no.nav.ereg

const val env_DEPLOY_APP = "DEPLOY_APP"
const val env_DEPLOY_CLUSTER = "DEPLOY_CLUSTER"
const val env_POSTER_SETTINGS = "POSTER_SETTINGS"
const val env_ACTIVE_DATES = "ACTIVE_DATES"

const val env_MS_BETWEEN_WORK = "MS_BETWEEN_WORK"

// Kafka environment dependencies
const val env_KAFKA_BROKERS = "KAFKA_BROKERS"
const val env_KAFKA_CLIENTID = "KAFKA_CLIENTID"
const val env_KAFKA_SECURITY = "KAFKA_SECURITY"
const val env_KAFKA_TOPIC = "KAFKA_TOPIC"
const val env_KAFKA_POLL_DURATION = "KAFKA_POLL_DURATION"
const val env_KAFKA_KEYSTORE_PATH = "KAFKA_KEYSTORE_PATH"
const val env_KAFKA_CREDSTORE_PASSWORD = "KAFKA_CREDSTORE_PASSWORD"
const val env_KAFKA_TRUSTSTORE_PATH = "KAFKA_TRUSTSTORE_PATH"
const val env_KAFKA_SCHEMA_REGISTRY = "KAFKA_SCHEMA_REGISTRY"
const val env_KAFKA_SCHEMA_REGISTRY_USER = "KAFKA_SCHEMA_REGISTRY_USER"
const val env_KAFKA_SCHEMA_REGISTRY_PASSWORD = "KAFKA_SCHEMA_REGISTRY_PASSWORD"

const val env_ARENA_HOST = "ARENA_HOST"

const val SF_PATH_oAuth = "/services/oauth2/token"

// Salesforce environment dependencies
const val env_SF_TOKENHOST = "SF_TOKENHOST"
const val env_SF_VERSION = "SF_VERSION"
const val env_HTTPS_PROXY = "HTTPS_PROXY"

// Salesforce required secrets
const val secret_SFClientID = "SFClientID"
const val secret_SFUsername = "SFUsername"

// Salesforce required secrets related to keystore for signed JWT
const val secret_keystoreJKSB64 = "keystoreJKSB64"
const val secret_KeystorePassword = "KeystorePassword"
const val secret_PrivateKeyAlias = "PrivateKeyAlias"
const val secret_PrivateKeyPassword = "PrivateKeyPassword"

const val numberOfSamplesInSampleRun = 3
const val hourToStartWorkSessionOnActiveDate = 8
const val SALESFORCE_VERSION = "v56.0"
