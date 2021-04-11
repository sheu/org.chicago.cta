package org.chicago.cta.producers.models

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import java.nio.file.Paths
import java.time.Instant

class ProducerStation(val stationId: Int,
                      val name: String,
                      val color: String,
                      val udacityProducer: UdacityProducer,
                      val pathToSchemaDir: String,
                      val pathToRiderShipDataDir: String,
                      val dirA: ProducerStation? = null,
                      var dirB: ProducerStation? = null,
                      var trainA: Train? = null,
                      var trainB: Train? = null,
                      ) {
    private val keySchema = Schema.Parser().setValidate(true).parse(Paths.get("$pathToSchemaDir/arrival_key.json").toFile())
    private val valueSchema = Schema.Parser().setValidate(true).parse(Paths.get("$pathToSchemaDir/arrival_value.json").toFile())
    private val stationName = name.toStationName()
    private val topicName = "org.chicago.cta.station.arrivals.${stationName}"
    val turnstile = Turnstile(topicName, pathToSchemaDir, topicName, stationId, pathToRiderShipDataDir, udacityProducer, color)

    fun run(train: Train, direction: String, prevStationId: Int, prevDirection: String) {
        val genericKeyData = GenericRecordBuilder(keySchema)
                .set("timestamp", Instant.now().toEpochMilli()).build()
        val genericValueData = GenericRecordBuilder(valueSchema).set("station_id", stationId)
                .set("direction", direction)
                .set("prev_station_id", prevStationId)
                .set("prev_direction", prevDirection)
                .set("train_status", train.status.status)
                .set("line", color)
                .set("train_id", train.trainId).build()

        udacityProducer.send(topicName, genericKeyData, genericValueData)

    }

    fun arriveA(train: Train, prevStationId: Int, prevDirection: String) {
        trainA = train
        this.run(train, "a", prevStationId, prevDirection)
    }

    fun arriveB(train: Train, prevStationId: Int, prevDirection: String) {
        trainB = train
        this.run(train, "b", prevStationId, prevDirection)
    }

    private fun String.toStationName(): String {
        return toLowerCase().replace("/", "_and_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("'", "")
    }

    fun close() {
        turnstile.close()
        udacityProducer.close()
    }


}