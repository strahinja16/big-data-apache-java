FROM maven:3.6.1-jdk-8-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY pom.xml ./
RUN mvn dependency:resolve
RUN mvn verify

COPY ./ ./
RUN mvn clean package

CMD java -cp target/streaming-producer-1.0-SNAPSHOT.jar Main