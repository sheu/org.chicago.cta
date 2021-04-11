package org.chicago.cta

import org.chicago.cta.producers.models.TurnstileHardware
import org.junit.jupiter.api.Test
import java.util.*

class TurnstileHardwareTest {
    @Test
    fun `test entries`() {
        val turnstileHardware = TurnstileHardware(40960,
                "/Users/sheugumbie/Projects/Learning/kafka/projects/org.chicago.cta/src/main/resources/data",
        )
        println(turnstileHardware.getEntries(Calendar.getInstance(), 54))
    }
}