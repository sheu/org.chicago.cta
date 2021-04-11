package org.chicago.cta.consumers.models

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
private val logger = KotlinLogging.logger { }
class Weather constructor(var name: String = "Default", var temperature: Float = 70.0f, var status: String = "sunny") : ConsumerRecordHandler{

    override fun processMessage(consumerRecord: ConsumerRecord<Any?, Any?>) {
        val message = consumerRecord.value() as Map<String, Any?>
        status = message["status"] as String
        temperature = message["temperature"] as Float

        logger.info { "Processed new temperature: $temperature" }

    }
}