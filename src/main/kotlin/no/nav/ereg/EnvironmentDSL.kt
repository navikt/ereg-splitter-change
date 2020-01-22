package no.nav.ereg

object EnvVarFactory {

    private var envVar_: EnvVar? = null
    val envVar: EnvVar
        get() {
            if (envVar_ == null) envVar_ = EnvVar()
            return envVar_ ?: throw AssertionError("Environment factory, null for environment variables!")
        }
}

data class EnvVar(
    // kafka details
    val kafkaBrokers: String = System.getenv("KAFKA_BROKERS")?.toString() ?: "",
    val kafkaClientID: String = System.getenv("KAFKA_CLIENTID")?.toString() ?: "",
    val kafkaSecurity: String = System.getenv("KAFKA_SECURITY")?.toString()?.toUpperCase() ?: "",
    val kafkaSecProt: String = System.getenv("KAFKA_SECPROT")?.toString() ?: "",
    val kafkaSaslMec: String = System.getenv("KAFKA_SASLMEC")?.toString() ?: "",
    val kafkaUser: String = System.getenv("KAFKA_USER")?.toString() ?: "",
    val kafkaPassword: String = System.getenv("KAFKA_PASSWORD")?.toString() ?: "",
    // see https://cwiki.apache.org/confluence/display/KAFKA/KIP-91+Provide+Intuitive+User+Timeouts+in+The+Producer
    val kafkaProducerTimeout: Int = System.getenv("KAFKA_PRODUCERTIMEOUT")?.toInt() ?: 31_000,
    val kafkaTopic: String = System.getenv("KAFKA_TOPIC")?.toString() ?: "",

    // ereg details - underenhet (UE) and (over)enhet (OE)
    val eregUEUrl: String = System.getenv("EREG_UEURL")?.toString() ?: "",
    val eregUEAccept: String = System.getenv("EREG_UEACCEPT")?.toString() ?: "",
    val eregOEUrl: String = System.getenv("EREG_OEURL")?.toString() ?: "",
    val eregOEAccept: String = System.getenv("EREG_OEACCEPT")?.toString() ?: "",
    val httpsProxy: String = System.getenv("HTTPS_PROXY")?.toString() ?: "",
    val noProxy: String = System.getenv("NO_PROXY")?.toString() ?: "",

    val runEachMorning: String = System.getenv("RUN_EACH_MORNING")?.toString()?.toUpperCase() ?: "FALSE",
    val maxAttempts: Int = System.getenv("MAX_ATTEMPTS")?.toInt() ?: 36,
    val msBetweenAttempts: Long = System.getenv("MS_BETWEEN_RETRIES")?.toLong() ?: 15 * 60 * 1_000
)

fun EnvVar.kafkaSecurityEnabled(): Boolean = kafkaSecurity == "TRUE"

fun EnvVar.kafkaSecurityComplete(): Boolean =
    kafkaSecProt.isNotEmpty() && kafkaSaslMec.isNotEmpty() && kafkaUser.isNotEmpty() && kafkaPassword.isNotEmpty()

fun EnvVar.eregDetailsComplete(): Boolean =
    eregUEUrl.isNotEmpty() && eregUEAccept.isNotEmpty() && eregOEUrl.isNotEmpty() && eregOEAccept.isNotEmpty()

fun EnvVar.runEachMorning(): Boolean = runEachMorning == "TRUE"
