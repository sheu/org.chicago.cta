package org.chicago.cta.producers.models

import org.chicago.cta.consumers.models.TrainStatus

data class Arrival constructor(val stationId: Int,
                               val direction: String,
                               val prevStationId: Int,
                               val prevDirection: String,
                               val trainId: String,
                               val trainStatus: TrainStatus,
                               val line: String)