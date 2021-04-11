package org.chicago.cta.producers.models

data class StationData(val stopId: Int,
                       val directionId: String,
                       val stop_name: String,
                       val stationName: String,
                       val stationDescriptiveName: String,
                       val stationId: Int,
                        val order: Int,
                       val red: Boolean,
                       val blue: Boolean,
                       val green: Boolean) {
}