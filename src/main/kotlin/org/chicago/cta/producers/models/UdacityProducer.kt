package org.chicago.cta.producers.models

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.*
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord


private val logger = KotlinLogging.logger { }

class UdacityProducer constructor(private val producer: Producer<GenericRecord, GenericRecord>) {


    fun close() {
        producer.flush()
        producer.close()
    }

    fun send(topicName: String, keyRecord: GenericRecord, valueRecord: GenericRecord) {
        println("Sending message to topic $topicName")
        producer.send(ProducerRecord(topicName, keyRecord, valueRecord)) { rm, e ->
            when (e) {
                null -> logger.info { "Produced: ${rm.serializedValueSize()} bytes" }
                else -> logger.error(e) { "Error while producing to topic: $topicName" }
            }
        }
    }
}