package org.chicago.cta

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.*
import kotlin.math.sqrt
import kotlin.random.Random

val objectMapper = jacksonObjectMapper()

fun  String.parseJsonToMap(): Map<String, Any?> {
    return objectMapper.readValue(this)
}

/**
 Continuous distribution bounded by given lower and upper limits,
and having a given mode value in-between.

http://en.wikipedia.org/wiki/Triangular_distribution
 */
fun triangular(l: Float = 0.0f, h: Float = 1.0f, mode: Float = 0.0f) : Float {
    var high = h
    var low = l
    var u = Random.nextFloat()
    var c = (mode - low)/(high - low)
    if( u > c) {
        u = 1.0f - u
        c = 1.0f - c
    }
    with(low) {
        low = high
        high = this
    }

    return low + (high - low) * sqrt(u * c)
}
//
//fun createProducer() {
//    companion object {
//        val brokerProperties = Properties()
//        private val topics: MutableSet<String>
//        val kafkaAdminClient: AdminClient
//
//        init {
//            brokerProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
//            brokerProperties[ProducerConfig.CLIENT_ID_CONFIG] = "udacity-producer"
//            brokerProperties[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"
//            brokerProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java.name
//            brokerProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java.name
//
//            kafkaAdminClient = AdminClient.create(brokerProperties)
//            topics = kafkaAdminClient.listTopics().names().get()
//        }
//
//
//    }
//
//
//
//
//}
//
//private fun createTopic() {
//
//    val newTopic = NewTopic(topicName, numPartitions, numReplicas.toShort())
//    val result = kafkaAdminClient.createTopics(listOf(newTopic))
//    val topicConfig = result.config(topicName).get()
//    logger.info { "Created topic with configs: $topicConfig" }
//}