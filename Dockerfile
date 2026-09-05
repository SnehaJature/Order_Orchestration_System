FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /app
COPY pom.xml .
COPY src src
RUN mvn package -DskipTests -q
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app/target/order-orchestration-system-0.1.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]