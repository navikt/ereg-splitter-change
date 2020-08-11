package no.nav.ereg

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import java.io.File
import java.io.InputStreamReader
import no.nav.ereg.proto.EregOrganisationEventKey
import no.nav.ereg.proto.EregOrganisationEventValue

class ERegDSLTests : StringSpec({

    // utility functions

    tailrec fun captureAllOrgs(
        isr: InputStreamReader,
        map: MutableMap<String, JsonOrgObject> = mutableMapOf()
    ): Map<String, JsonOrgObject> {

        val joo = isr.captureJsonOrgObject()

        return if (!joo.isOk()) map.toMap()
        else {
            map[joo.orgNo] = joo
            captureAllOrgs(isr, map)
        }
    }

    tailrec fun <K, V> iterateAllOrgs(
        iter: Iterator<KafkaPayload<K, V>>,
        noOfElements: Int = 0
    ): Int =
        if (!iter.hasNext()) noOfElements
        else {
            iter.next()
            iterateAllOrgs(iter, noOfElements + 1)
        }

    val underenheter = captureAllOrgs(InputStreamReader(File(FILUENHETER).inputStream()))

    // the last two organisations have the org.no at different positions in main body
    val enheter = captureAllOrgs(InputStreamReader(File(FILOENHETER).inputStream()))

    "EregDSL should split Json array of underenheter to orgs objects" {
        underenheter.size shouldBe 5
        underenheter.keys.toSet().size shouldBe 5
    }

    val org981452607 = """{
  "organisasjonsnummer" : "981452607",
  "navn" : "TYSNES LAKK OG SERVICESENTER AS",
  "organisasjonsform" : {
    "kode" : "BEDR",
    "beskrivelse" : "Bedrift",
    "links" : [ ]
  },
  "registreringsdatoEnhetsregisteret" : "2000-01-13",
  "registrertIMvaregisteret" : false,
  "naeringskode1" : {
    "beskrivelse" : "Detaljhandel med drivstoff til motorvogner",
    "kode" : "47.300"
  },
  "antallAnsatte" : 4,
  "overordnetEnhet" : "981442075",
  "oppstartsdato" : "2000-02-01",
  "beliggenhetsadresse" : {
    "land" : "Norge",
    "landkode" : "NO",
    "postnummer" : "5680",
    "poststed" : "TYSNES",
    "adresse" : [ ],
    "kommune" : "TYSNES",
    "kommunenummer" : "1223"
  },
  "links" : [ ]
}"""

    "EregDSL should give identical hash code for two identical underheter (json text wise)" {
        underenheter[underenheter.keys.first()] shouldBe JsonOrgObject(
            StreamState.STREAM_ONGOING, org981452607, "981452607", org981452607.hashCode()
        )
    }

    val org981452607Changed = """{
  "organisasjonsnummer" : "981452607",
  "navn" : "TYSNES LAKK & SERVICESENTER AS",
  "organisasjonsform" : {
    "kode" : "BEDR",
    "beskrivelse" : "Bedrift",
    "links" : [ ]
  },
  "registreringsdatoEnhetsregisteret" : "2000-01-13",
  "registrertIMvaregisteret" : false,
  "naeringskode1" : {
    "beskrivelse" : "Detaljhandel med drivstoff til motorvogner",
    "kode" : "47.300"
  },
  "antallAnsatte" : 4,
  "overordnetEnhet" : "981442075",
  "oppstartsdato" : "2000-02-01",
  "beliggenhetsadresse" : {
    "land" : "Norge",
    "landkode" : "NO",
    "postnummer" : "5680",
    "poststed" : "TYSNES",
    "adresse" : [ ],
    "kommune" : "TYSNES",
    "kommunenummer" : "1223"
  },
  "links" : [ ]
}"""

    "EregDSL should give different hash code for any differences for a underenhet (json text-wise)" {
        underenheter[underenheter.keys.first()] shouldNotBe JsonOrgObject(
            StreamState.STREAM_ONGOING, org981452607Changed, "981452607", org981452607Changed.hashCode()
        )
    }

    "EregDSL should give default objects when invalid ByteArray for Key and Value, thus empty orgno" {

        "invalid".toByteArray().protobufSafeParseKey() shouldBe EregOrganisationEventKey.getDefaultInstance()
        "invalid".toByteArray().protobufSafeParseValue() shouldBe EregOrganisationEventValue.getDefaultInstance()
        EregOrganisationEventKey.getDefaultInstance().orgNumber shouldBe ""
    }

    "EregDSL should map JsonOrgObject (underenhet) correctly to KafkaPayload" {
        val kv1 = underenheter[underenheter.keys.first()]?.toKafkaPayload(EREGEntityType.UNDERENHET)
        val kv2 = KafkaPayload<ByteArray, ByteArray>(
            EregOrganisationEventKey.newBuilder().apply {
                this.orgNumber = "981452607"
                this.orgType = EregOrganisationEventKey.OrgType.UNDERENHET
            }.build().toByteArray(),
            EregOrganisationEventValue.newBuilder().apply {
                this.orgAsJson = org981452607
                this.jsonHashCode = org981452607.hashCode()
            }.build().toByteArray()
        )

        kv1?.key shouldBe kv2.key
        kv1?.value shouldBe kv2.value
    }

    "EregDSL should give a set of correct KafkaPayload for underenhet" {
        val s1 = InputStreamReader(File(FILUENHETER).inputStream())
            .asFilteredSequence(EREGEntityType.UNDERENHET, emptyMap())
            .toSet()
        val s2 = underenheter.map { it.value.toKafkaPayload(EREGEntityType.UNDERENHET) }.toSet()

        s1.size shouldBe s2.size
    }

    "EregDSL should give a set of correct KafkaPayload for changed underenheter" {
        val cache = InputStreamReader(File(FILUENHETER).inputStream())
            .asFilteredSequence(EREGEntityType.UNDERENHET, emptyMap())
            .map { it.key.protobufSafeParseKey().orgNumber to it.value.protobufSafeParseValue().jsonHashCode }
            .toMap()

        val changes = InputStreamReader(File(FILUENHETERCHANGED).inputStream())
            .asFilteredSequence(EREGEntityType.UNDERENHET, cache)
            .toSet()

        // FILUENHETERCHANGED contains 3 changes compared to FILUENHETER
        changes.size shouldBe 3
    }

    "EregDSL should split Json array of enheter to orgs objects" {
        enheter.size shouldBe 5
        enheter.keys.toSet().size shouldBe 5
    }

    val org899176812 = """{
  "organisasjonsnummer" : "899176812",
  "navn" : "BASTIAN STOERMANN-NÆSS",
  "organisasjonsform" : {
    "kode" : "ENK",
    "beskrivelse" : "Enkeltpersonforetak",
    "links" : [ ]
  },
  "registreringsdatoEnhetsregisteret" : "2012-11-24",
  "registrertIMvaregisteret" : false,
  "naeringskode1" : {
    "beskrivelse" : "Undervisning innen idrett og rekreasjon",
    "kode" : "85.510"
  },
  "antallAnsatte" : 0,
  "forretningsadresse" : {
    "land" : "Norge",
    "landkode" : "NO",
    "postnummer" : "0584",
    "poststed" : "OSLO",
    "adresse" : [ "Krokliveien 55" ],
    "kommune" : "OSLO",
    "kommunenummer" : "0301"
  },
  "institusjonellSektorkode" : {
    "kode" : "8200",
    "beskrivelse" : "Personlig næringsdrivende"
  },
  "registrertIForetaksregisteret" : false,
  "registrertIStiftelsesregisteret" : false,
  "registrertIFrivillighetsregisteret" : false,
  "konkurs" : false,
  "underAvvikling" : false,
  "underTvangsavviklingEllerTvangsopplosning" : false,
  "maalform" : "Bokmål",
  "links" : [ ]
}"""

    "EregDSL should map JsonOrgObject (enhet) correctly to KafkaPayload" {
        val testEnt = enheter[enheter.keys.first()]
        val kv1 = enheter[enheter.keys.first()]?.toKafkaPayload(EREGEntityType.ENHET)
        val kv2 = KafkaPayload<ByteArray, ByteArray>(
            EregOrganisationEventKey.newBuilder().apply {
                this.orgNumber = "899176812"
                this.orgType = EregOrganisationEventKey.OrgType.ENHET
            }.build().toByteArray(),
            EregOrganisationEventValue.newBuilder().apply {
                this.orgAsJson = org899176812
                this.jsonHashCode = org899176812.hashCode()
            }.build().toByteArray()
        )

        kv1?.key shouldBe kv2.key
        kv1?.value shouldBe kv2.value
    }

    "EregDSL should give a set of correct KafkaPayload for changed enheter" {
        val cache = InputStreamReader(File(FILOENHETER).inputStream())
            .asFilteredSequence(EREGEntityType.ENHET, emptyMap())
            .map { it.key.protobufSafeParseKey().orgNumber to it.value.protobufSafeParseValue().jsonHashCode }
            .toMap()

        val changes = InputStreamReader(File(FILOENHETERCHANGED).inputStream())
            .asFilteredSequence(EREGEntityType.ENHET, cache)
            .toSet()

        // FILOENHETERCHANGED contains 2 changes compared to FILOENHETER
        changes.size shouldBe 2
    }

    "EregDSL should invoke doConsume when client and server is working" {

        eregAPI(9000) { ueURL, oeURL ->
            EREGEntity(EREGEntityType.UNDERENHET, ueURL, "")
                .getJsonAsSequenceIterator(emptyMap()) { seqIter -> iterateAllOrgs(seqIter) shouldBe 5 } shouldBe true

            EREGEntity(EREGEntityType.ENHET, oeURL, "")
                .getJsonAsSequenceIterator(emptyMap()) { seqIter -> iterateAllOrgs(seqIter) shouldBe 5 } shouldBe true
        }
    }

    "EregDSL should not invoke doConsume when server error (invalid files)" {

        // pointing to non-existing files to download as stream
        eregAPI(9000, "na", "na") { ueURL, oeURL ->
            EREGEntity(EREGEntityType.UNDERENHET, ueURL, "")
                .getJsonAsSequenceIterator(emptyMap()) {
                    // no invocation
                    true shouldBe false
                } shouldBe false

            EREGEntity(EREGEntityType.ENHET, oeURL, "")
                .getJsonAsSequenceIterator(emptyMap()) {
                    // no invocation
                    true shouldBe false
                } shouldBe false
        }
    }

    "EregDSL should not invoke doConsume when client error (invalid url)" {

        eregAPI(9000) { _, _ ->
            EREGEntity(EREGEntityType.UNDERENHET, "na", "")
                .getJsonAsSequenceIterator(emptyMap()) {
                    // no invocation
                    true shouldBe false
                } shouldBe false

            EREGEntity(EREGEntityType.ENHET, "na", "")
                .getJsonAsSequenceIterator(emptyMap()) {
                    // no invocation
                    true shouldBe false
                } shouldBe false
        }
    }
})
