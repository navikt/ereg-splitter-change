FROM navikt/java:11
ENV JAVA_OPTS="-Dlogback.configurationFile=logback-remote.xml -Xms2048m', '-Xmx4096m"
COPY build/libs/app*.jar app.jar