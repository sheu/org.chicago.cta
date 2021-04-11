package org.chicago.cta.producers.models

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.chicago.cta.triangular
import java.lang.Float.max
import java.lang.Float.min
import java.nio.file.Paths
import java.time.Instant

class Weather constructor(val month: Int, val udacityProducer: UdacityProducer, val pathToSchemaDir: String) {
    enum class Status(val status: String) {
        SUNNY("sunny"),
        PARTLY_CLOUDY("partly_cloud"),
        CLOUDY("cloudy"),
        WINDY("windy"),
        PRECIPITATION("precipitation")


    }


    var status: Status = Status.SUNNY
    var temp: Float = 0.0f
    val keySchema = Schema.Parser().setValidate(true).parse(Paths.get("${pathToSchemaDir}/weather_key.json").toFile())
    val valueSchema = Schema.Parser().setValidate(true).parse(Paths.get("${pathToSchemaDir}/weather_value.json").toFile())
    val winterMonts = setOf<Int>(0, 1, 2, 3, 10, 11)
    val summerMonts = setOf<Int>(6, 7, 8)

    init {
        if (month in winterMonts) {
            temp = 40.0f
        } else {
            temp = 85.0f
        }
    }


     fun setWeather(m: Int) {
        var mode = 0.0f
        if (m in winterMonts) {
            mode = -1.0f
        } else if (m in summerMonts) {
            mode = 1.0f
        }
        temp += min(max(-20.0f, triangular(-10.0f, 10.0f, mode)), 100.0f)
        status = Status.values().toList().shuffled().take(1).first()
    }

    fun run() {
        val genericKeyData = GenericRecordBuilder(keySchema)
                .set("timestamp", Instant.now().toEpochMilli()).build()
        val genericValueData = GenericRecordBuilder(valueSchema).set("temperature", temp)
                .set("status", status.status).build()

        udacityProducer.send("org.chicago.cta.weather.events",genericKeyData, genericValueData)
    }


}