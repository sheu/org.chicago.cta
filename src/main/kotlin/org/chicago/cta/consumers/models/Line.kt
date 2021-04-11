package org.chicago.cta.consumers.models

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord

private val logger = KotlinLogging.logger { }

class Line constructor(val color: String, var stationIdToStationMap: MutableMap<Int, Station> = mutableMapOf(), var colorCode: String = "") : ConsumerRecordHandler {
    init {
        colorCode = when (color) {
            "blue" -> {
                "#1E90FF"
            }
            "red" -> {
                "#DC143C"
            }
            "green" -> {
                "#32CD32"
            }
            else -> {
                "0xFFFFFF"
            }
        }
    }

    fun handleStation(lineToColorMap: Map<String, String>) {
        if (lineToColorMap["line"] != color) return
        stationIdToStationMap[lineToColorMap["station_id"]!!.toInt()] = fromMessage(lineToColorMap)
    }

    fun handleArrival(consumerRecord: ConsumerRecord<Any?, Any?>) {
        val message = consumerRecord.value() as Map<String, Any?>
        val previousStationId = message["prev_station_id"]
        val previousDir = message["prev_direction"]
        if (previousDir != null && previousStationId != null) {
            val previousStation = stationIdToStationMap[previousStationId]
            if (previousStation != null) {
                previousStation.handleDeparture(previousDir as String)
            } else {
                logger.info { "Unable to handle previous station due to missing station" }
            }
        } else {
            logger.info { "Unable to handle previous station due to missing previous info" }
        }
        val stationId = message["station_id"] as Int
        val station = stationIdToStationMap[stationId]
        if (station == null) {
            logger.info { "Unable to handle message due to missing station" }
            return
        }
        station.handleArrival(message["direction"] as String, message["train_id"] as Int, message["train_status"] as String)

    }

    override fun processMessage(consumerRecord: ConsumerRecord<Any?, Any?>) {
        if (consumerRecord.topic() == "org.chicago.cta.stations.table.v1") {
            try {
                val value = consumerRecord.value() as Map<String, String>
                this.handleStation(value)
            } catch (e: Exception) {
                logger.error(e) { " Bad station ${consumerRecord.value()}" }
            }
        } else if (consumerRecord.topic().contains("org.chicago.cta.station") && consumerRecord.topic().contains("arrivals")) {
            this.handleArrival(consumerRecord)
        } else if (consumerRecord.topic() == "TURNSTILE_SUMMARY_STREAM") {
            val jsonData = consumerRecord.value() as Map<String, Any?>
            val stationId = jsonData["STATION_ID"]
            val station = stationIdToStationMap[stationId]
            if (station == null) {
                logger.info { "Unable to handle message due to missing station" }
                return
            }
            station.processMessage(jsonData as Map<String, Int>)
        }
    }
}