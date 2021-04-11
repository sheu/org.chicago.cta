package org.chicago.cta.consumers

import io.confluent.ksql.api.client.Client
import io.confluent.ksql.api.client.ClientOptions
import io.confluent.ksql.api.client.ExecuteStatementResult
import mu.KotlinLogging

import java.util.Collections


private val logger = KotlinLogging.logger { }

class Ksql {
    val KSQLDB_SERVER_HOST = "localhost"
    val KSQLDB_SERVER_HOST_PORT = 8088

    fun run() {
        val createTurnstileStream = """
                                CREATE stream turnstilestream (station_id BIGINT, 
                                station_name VARCHAR, line VARCHAR) 
                                WITH (KAFKA_TOPIC='org.cta.raw.turnstile', VALUE_FORMAT='AVRO');
                                """
        val createTurnstileTable = """
                CREATE TABLE TURNSTILE_SUMMARY_STREAM   
                WITH (VALUE_FORMAT='JSON') AS SELECT station_id as station_id, 
                COUNT(station_id) as count FROM turnstilestream GROUP BY station_id;
                """

        val options: ClientOptions = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_HOST_PORT)
        val client: Client = Client.create(options)
        val tables = client.listTables().get().filter { it.name == "TURNSTILE_SUMMARY_STREAM" }
        if(tables.isNotEmpty()) {
            logger.info { "Not creating anything as the table already exists" }
            return
        }

        val properties = mapOf("auto.offset.reset" to "earliest")
        val streamResult = client.executeStatement(createTurnstileStream, properties).get()
        logger.info {"Stream query id: ${streamResult.queryId()}"}
            val tableResult = client.executeStatement(createTurnstileTable, properties).get()
            logger.info {"Table query id: ${tableResult.queryId()}"}
    }
}