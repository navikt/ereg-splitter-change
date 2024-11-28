FROM ghcr.io/navikt/baseimages/temurin:17
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms10G -Xmx16G"
COPY build/libs/app*.jar app.jar