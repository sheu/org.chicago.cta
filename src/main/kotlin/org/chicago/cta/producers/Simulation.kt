package org.chicago.cta.producers

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import kotlinx.coroutines.delay
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.chicago.cta.producers.models.ProducerLine
import org.chicago.cta.producers.models.StationData
import org.chicago.cta.producers.models.UdacityProducer
import org.chicago.cta.producers.models.Weather
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.time.Duration
import org.apache.kafka.clients.producer.ProducerConfig




private val logger = KotlinLogging.logger { }

class Simulation(val sleepInSeconds: Int,
                 val pathToStationDataDir: String,
                 val timeStep: Int,
                 val udacityProducer: UdacityProducer,
                 val pathToSchemaDir: String,
                 val pathToRidershipDir: String) {

    enum class Weekdays(val day: String) {
        MONDAY("mon"),
        TUESDAY("tue"),
        WEDNESDAY("wed"),
        THURSDAY("thur"),
        FRIDAY("fri"),
        SATURDAY("sat"),
        SUNDAY("sun")
    }

    val listStationData = readDataFomDisk()
    val trainLines = listOf(
            ProducerLine(ProducerLine.Color.BLUE,
                    listStationData.filter { it.blue },
                    udacityProducer,
                    pathToSchemaDir,
                    pathToRidershipDir),
            ProducerLine(ProducerLine.Color.BLUE,
                    listStationData.filter { it.red },
                    udacityProducer,
                    pathToSchemaDir,
                    pathToRidershipDir),
            ProducerLine(ProducerLine.Color.BLUE,
                    listStationData.filter { it.green },
                    udacityProducer,
                    pathToSchemaDir,
                    pathToRidershipDir))


    fun readDataFomDisk(): List<StationData> {

        return Files.readAllLines(Paths.get("${pathToStationDataDir}/cta_stations.csv"))
                .map { it.split(",") }
                .map { StationData(it[0].toInt(), it[1], it[2], it[3], it[4], it[5].toInt(), it[6].toInt(), it[7].toBoolean(), it[8].toBoolean(), it[9].toBoolean()) }

    }

    suspend fun run() {
        val currentTime = Calendar.getInstance()
        logger.info("Beginning simulation, press Ctrl+C to exit at any time")
        logger.info("beginning cta train simulation")
        val weather = Weather(currentTime.get(Calendar.MONTH),  udacityProducer, pathToSchemaDir)
        while (true) {
            logger.info { "Simulation running" }
            if(currentTime.get(Calendar.MINUTE) == 0) {
                weather.setWeather(currentTime.get(Calendar.MONTH))
                weather.run()
            }
            trainLines.forEach { it.run(currentTime, timeStep) }
            currentTime.add(Calendar.SECOND, timeStep)
            delay(sleepInSeconds.toLong() * 1000)
        }

    }

    fun close() {
        trainLines.forEach { it.close() }
    }


}

suspend fun main(args: Array<String>) {

    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
    props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,] = "http://localhost:8081"
    props[ProducerConfig.ACKS_CONFIG] = "1"
    val udacityProducer = UdacityProducer(KafkaProducer(props))
    val simulation = Simulation(args[0].toInt(), args[1], args[2].toInt(), udacityProducer, args[3], args[4])
    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info { "Closing down!" }
        simulation.close()
    })
    simulation.run()
}