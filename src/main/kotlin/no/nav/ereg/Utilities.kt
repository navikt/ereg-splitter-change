package no.nav.ereg

import com.google.protobuf.InvalidProtocolBufferException
import mu.KotlinLogging
import no.nav.ereg.proto.EregOrganisationEventKey
import no.nav.ereg.proto.EregOrganisationEventValue

private val log = KotlinLogging.logger {}

internal fun ByteArray.protobufSafeParseKey(): EregOrganisationEventKey = this.let { ba ->
    try {
        EregOrganisationEventKey.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        ServerState.state = ServerStates.ProtobufIssues
        log.error { "Failure when parsing protobuf for kafka key - ${e.message}" }
        EregOrganisationEventKey.getDefaultInstance()
    }
}

internal fun ByteArray.protobufSafeParseValue(): EregOrganisationEventValue = this.let { ba ->
    try {
        EregOrganisationEventValue.parseFrom(ba)
    } catch (e: InvalidProtocolBufferException) {
        ServerState.state = ServerStates.ProtobufIssues
        log.error { "Failure when parsing protobuf for kafka value - ${e.message}" }
        EregOrganisationEventValue.getDefaultInstance()
    }
}
