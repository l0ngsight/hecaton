FROM eclipse-temurin:21-jre
COPY target/embedded-member-1.0.jar /opt/app/app.jar
COPY src/main/resources/hazelcast.xml /opt/app/hazelcast.xml
ENV JAVA_OPTS="-Dhazelcast.config=/opt/app/hazelcast.xml -Dhz.network.port.port=5701"
ENV HZ_CLUSTERNAME=migration-test
CMD ["sh", "-c", "java $JAVA_OPTS -jar /opt/app/app.jar"]
