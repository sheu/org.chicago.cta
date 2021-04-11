package org.chicago.cta.consumers

import kotlinx.coroutines.delay
import kotlinx.coroutines.yield
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.chicago.cta.consumers.models.ConsumerRecordHandler
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration

import java.util.*


private val logger = KotlinLogging.logger { }

class Consumer constructor(val topicNamePattern: String,
                           val consumerRecordHandler: ConsumerRecordHandler,
                           private val isAvro: Boolean = true,
                           private val offsetEarliest: Boolean = false,
                           val sleepInSeconds: Int = 1,
                           val consumeTimeout: Int = 1) {


    private val brokerProperties: Properties = Properties()
    private val consumer: KafkaConsumer<Any, Any>

    init {
        brokerProperties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        brokerProperties[ConsumerConfig.GROUP_ID_CONFIG] = "com.udacity.kotlin.project"
        if (offsetEarliest)
            brokerProperties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        if (isAvro) {
            brokerProperties["schema.registry.url"] = "http://localhost:8081"

        }
        consumer = KafkaConsumer(brokerProperties)
        consumer.subscribe(listOf(topicNamePattern))
    }

    suspend fun run() {

        while (true) {
            var numResults = 1
            while (numResults > 0) {
                numResults = this.consume()
            }
            delay(sleepInSeconds.toLong() * 1000)

        }
    }

    private fun consume(): Int {
        val records = consumer.poll(Duration.ofSeconds(consumeTimeout.toLong()))
        return if (records.isEmpty) {
            0
        } else {
            for (record in records) {
                consumerRecordHandler.processMessage(record)
            }
            logger.info("Consumed: ${records.count()} messages from: $topicNamePattern")
            1
        }
    }

    fun close() {
        consumer.close()
    }

}