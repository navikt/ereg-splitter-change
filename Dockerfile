FROM ghcr.io/navikt/baseimages/temurin:17
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms512M -Xmx8192M"
COPY build/libs/app*.jar app.jar