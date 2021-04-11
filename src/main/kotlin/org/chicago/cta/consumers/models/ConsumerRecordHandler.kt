package org.chicago.cta.consumers.models

import org.apache.kafka.clients.consumer.ConsumerRecord

interface ConsumerRecordHandler {
    fun processMessage(consumerRecord: ConsumerRecord<Any?, Any?>)
}