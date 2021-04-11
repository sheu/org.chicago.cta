package org.chicago.cta.producers.models

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder
import java.nio.file.Paths
import java.time.Instant
import java.util.*

class Turnstile constructor(private val stationName: String,
                            pathToSchemaDir: String,
                            private val topicName: String,
                            private val stationId: Int,
                            pathToRidershipDataDir: String,
                           private val udacityProducer: UdacityProducer, val line: String) {
    private val keySchema = Schema.Parser().setValidate(true).parse(Paths.get("$pathToSchemaDir/turnstile_key.json").toFile())
    private val valueSchema = Schema.Parser().setValidate(true).parse(Paths.get("$pathToSchemaDir/turnstile_value.json").toFile())
    private val turnstileHardware = TurnstileHardware(stationId, pathToRidershipDataDir)

    fun run(calendar: Calendar, totalTime: Long) {
        val numEntries = turnstileHardware.getEntries(calendar, totalTime).toInt()



        for (i in 0..numEntries) {
            val genericKeyData = GenericRecordBuilder(keySchema)
                    .set("timestamp", Instant.now().toEpochMilli()).build()
            val genericValueData = GenericRecordBuilder(valueSchema).set("station_id", stationId)
                    .set("line", line).set("station_name", stationName).build()
            udacityProducer.send(topicName, genericKeyData, genericValueData)
        }


    }

    fun close() {
        udacityProducer.close()
    }

}