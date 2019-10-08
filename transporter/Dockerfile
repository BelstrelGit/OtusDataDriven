FROM openjdk:8
COPY ./target /usr/bin/app
COPY  ./stage/  /transporter/stage/
WORKDIR /usr/bin/app
ENTRYPOINT java -jar transporter-1.0-SNAPSHOT-uber.jar com.belstrel.otusanalytics.transporter.DataTransporter 



