# Transit Sims in Spring Boot: the executable jar, extracted, in a JRE image. An executable jar reads its nested jars at
# every start; extracted, the jar has its dependencies in lib next to it ("Efficient Deployments" of Spring Boot).
FROM eclipse-temurin:25-jre AS extract
COPY target/transitsims.jar /tmp/
RUN java -Djarmode=tools -jar /tmp/transitsims.jar extract --destination /app

FROM eclipse-temurin:25-jre
COPY --from=extract /app /app
CMD ["java", "-jar", "/app/transitsims.jar"]
