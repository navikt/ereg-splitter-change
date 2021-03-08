FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms8192M -Xmx8192M"
COPY build/libs/app*.jar app.jar