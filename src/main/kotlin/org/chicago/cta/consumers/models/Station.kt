package org.chicago.cta.consumers.models

class Station constructor(val stationId: Int,
                          val stationName: String,
                          val order: String,
                          var dirA: TrainStatus? = null,
                          var dirB: TrainStatus? = null,
                          var numTurnstileEntries: Int = 0)  {
    fun handleDeparture(direction: String) {
        if(direction == "a") this.dirA = null
        else this.dirB = null
    }

    fun handleArrival(direction: String, trainId: Int, trainStatusString: String) {
        val trainStatus  = TrainStatus(trainId, trainStatusString)
        if(direction == "a") this.dirA = trainStatus
        else this.dirB = trainStatus
    }

    fun processMessage(turnstileCountMap: Map<String, Int>) {
        numTurnstileEntries = turnstileCountMap["COUNT"]!!
    }

}

fun fromMessage(valuesMap: Map<String, Any>) =
        Station(valuesMap["station_id"] as Int, valuesMap["station_Name"] as String, valuesMap["order"] as String)

