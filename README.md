# Kafka Streaming Simulation of Chicago CTA

This is conversion of the project I did as part of the Data Stream nano degree
from [Udacity](https://www.udacity.com/)

The course was teaches data streaming with Apache kafka and Spark.  This was the first
part that only covered apache kafka.

Most of the code is not Kotlin idiomatic but that's a challenge for another day.
Here you can learn how to use kotlin to write clients for apache kafka.

# How to Run it

1. Ensure you kafka cluster running if not there is docker compose included in 
   docker folder of this project. The file depends on the docker connector image built
   by the provide ``Dockerfile``
2. Assuming you managed to get kafka cluster running
    ``` mvn clean package```
3. ```java -jar target/udacity-jar-with-dependencies.jar 5 <Path to data folder> 2 <Path to schema folder>  < path to data folder>```

