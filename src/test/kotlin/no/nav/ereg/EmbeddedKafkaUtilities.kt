package no.nav.ereg

import no.nav.common.KafkaEnvironment
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType

// utility function for setting access control list
internal fun KafkaEnvironment.addProducerToTopic(username: String, topic: String) = this.let { ke ->
    ke.adminClient?.createAcls(
        listOf(AclOperation.DESCRIBE, AclOperation.WRITE, AclOperation.CREATE)
            .map { aclOp ->
                AclBinding(
                    ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                    AccessControlEntry("User:$username", "*", aclOp, AclPermissionType.ALLOW)
                )
            }
    )
    ke
}

internal fun KafkaEnvironment.addConsumerToTopic(username: String, topic: String) = this.let { ke ->
    ke.adminClient?.createAcls(
        listOf(AclOperation.DESCRIBE, AclOperation.READ)
            .map { aclOp ->
                AclBinding(
                    ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                    AccessControlEntry("User:$username", "*", aclOp, AclPermissionType.ALLOW)
                )
            }
    )
    ke
}
