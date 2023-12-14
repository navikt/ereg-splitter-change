# ereg_splitter_change

A microservice publishing all 'enhetsregiseter' changes (Brønnøysund) to kafka compaction log 'public-ereg-cache-org-json'.

Each event is described by the following protobuf 3 specification
```proto
// this message will be the key part of kafka payload
message EregOrganisationEventKey {

  string org_number = 1;

  enum OrgType {
    ENHET = 0;
    UNDERENHET = 1;
  }
  OrgType org_type = 2;
}

// this message will be the value part of kafka payload
message EregOrganisationEventValue {

  string org_as_json = 1;
  int32 json_hash_code = 2;
}
```

## Tools
- Kotlin
- Gradle
- Kotlintest test framework

## Components
- Kafka client
- Http4K
- Protobuf 3

## Build
```
./gradlew clean build installDist
```


For internal resources, send request/questions to slack #crm-plattform-team 