package org.chicago.cta

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.MockProducer
import org.chicago.cta.producers.models.UdacityProducer
import org.chicago.cta.producers.models.Weather
import org.junit.jupiter.api.Test
import java.nio.file.Paths
import kotlin.test.assertNotNull

class ProducerTest {
    @Test
    fun `successful weather message`() {

        val mockProducer = MockProducer<GenericRecord, GenericRecord>()
        val producer = UdacityProducer(mockProducer)
        val weather = Weather(1,
                producer,
                "/Users/sheugumbie/Projects/Learning/kafka/projects/org.chicago.cta/src/main/resources/schemas")
        weather.run()
        val result = mockProducer.history().last()
        assertNotNull(result.key().get("timestamp"))
        assertNotNull(result.value().get("temperature"))
        assertNotNull(result.value().get("status"))

    }
}