FROM ghcr.io/navikt/baseimages/temurin:17
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms1G -Xmx14G -XX:MaxMetaspaceSize=256M"
COPY build/libs/app*.jar app.jar