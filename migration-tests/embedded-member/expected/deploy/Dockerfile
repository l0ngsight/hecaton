FROM eclipse-temurin:21-jre
COPY target/embedded-member-1.0.jar /opt/app/app.jar
COPY src/main/resources/hecaton.xml /opt/app/hecaton.xml
ENV JAVA_OPTS="-Dhecaton.config=/opt/app/hecaton.xml -Dhc.network.port.port=5701"
ENV HC_CLUSTERNAME=migration-test
CMD ["sh", "-c", "java $JAVA_OPTS -jar /opt/app/app.jar"]
