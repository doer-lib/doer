# Transit Sims in Helidon MP: the jar and its dependencies in libs, in a JRE image
FROM eclipse-temurin:25-jre
COPY target/libs /app/libs
COPY target/transitsims-helidon.jar /app/
CMD ["java", "-jar", "/app/transitsims-helidon.jar"]
