package org.chicago.cta.consumers.models

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.chicago.cta.parseJsonToMap

private val logger = KotlinLogging.logger { }
class Lines constructor(private val redLine: Line = Line("red"),
                        private val greeLine: Line = Line("green"),
                        private val blueLine: Line = Line("blue"),
                        val name: String = "Default"): ConsumerRecordHandler {

    override fun processMessage(consumerRecord: ConsumerRecord<Any?, Any?>) {
        if (consumerRecord.topic().contains("org.chicago.cta.station")) {
            val message =  if(consumerRecord.topic() == "org.chicago.cta.stations.table.v1")
                    (consumerRecord.value() as String).parseJsonToMap()
            else
                consumerRecord.value() as Map<String, Any?>
            when {
                message["line"] == "green" -> greeLine.processMessage(consumerRecord)
                message["line"] == "red" -> redLine.processMessage(consumerRecord)
                message["line"] == "blue" -> blueLine.processMessage(consumerRecord)
                else -> logger.info("Discarding unknown line message: ${message["line"]}")
            }
        } else {
            logger.info("Ignoring none line message from ${consumerRecord.topic()}")
        }
    }

}