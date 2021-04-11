package org.chicago.cta.producers.models

import java.util.*
import kotlin.math.abs

class ProducerLine(val color: Color,
                   val stationData: List<StationData>,
                   val udacityProducer: UdacityProducer,
                   val pathToSchemaDir: String,
                   val pathToRiderShipDataDir: String,
                   val numTrains: Int = 10) {

    val stations = buildLineData(stationData)
    val numStations = stations.size - 1


    enum class Color(val color: String) {
        BLUE("blue"),
        GREEN("green"),
        RED("red")
    }

    companion object {
        val numDirections = 2
    }


    fun buildLineData(stationData: List<StationData>): List<ProducerStation> {
        println("Size before distinct: ${stationData.size}")
        val distinctStationByNameAndId = stationData.distinctBy { it.stationName }.distinctBy { it.stationId }
        println("Size after distinct: ${distinctStationByNameAndId.size}")
        val f = distinctStationByNameAndId.first()
        val line = mutableListOf<ProducerStation>()

        var prevStation = ProducerStation(f.stationId,
                f.stationName,
                this.color.color,
                udacityProducer,
                pathToSchemaDir,
                pathToRiderShipDataDir)
        line.add(prevStation)
        for (i in distinctStationByNameAndId.drop(1)) {
            val newStation = ProducerStation(i.stationId,
                    i.stationName,
                    this.color.color,
                    udacityProducer,
                    pathToSchemaDir,
                    pathToRiderShipDataDir,
                    prevStation
            )
            prevStation.dirB = newStation
            prevStation = newStation
            line.add(newStation)

        }
        return line
    }

    fun buildTrains(): List<Train> {
        val trains = mutableListOf<Train>()
        var currentLocation = 0
        var bDirection = true
        for (trainId in 0..numTrains) {
            val train = Train("${color.color.toUpperCase()}L${trainId.toString().padStart(3, '0')}", Train.Status.IN_SERVICE)
            trains.add(train)
            if (bDirection) {
                stations[currentLocation].arriveB(train, -1, "")
            } else {
                stations[currentLocation].arriveA(train, -1, "")
            }
            val newValuePair = getNextIndex(currentLocation, bDirection)
            currentLocation = newValuePair.first
            bDirection = newValuePair.second
        }
        return trains.toList()

    }

    fun getNextIndex(currentIndex: Int, bDirection: Boolean, stepSize: Int = -1): Pair<Int, Boolean> {
        val newStepSize = if (stepSize > 0) stepSize else (numStations * numDirections) / numTrains
        var nextIndex: Int
        return if (bDirection) {
            nextIndex = currentIndex + stepSize
            if (nextIndex < numStations) {
                nextIndex to true
            } else {
                (numStations - (nextIndex % numStations)) to false
            }
        } else {
            nextIndex = currentIndex - stepSize
            if (nextIndex > 0) {
                nextIndex to false
            } else {
                abs(nextIndex) to true
            }
        }

    }

    fun run(timestamp: Calendar, timeStep: Int) {
        advanceTurnstiles(timestamp, timeStep)
        advanceTrains()
    }

    fun advanceTurnstiles(timestamp: Calendar, timeStep: Int) {
        stations.forEach { it.turnstile.run(timestamp, timeStep.toLong()) }
    }

    fun advanceTrains() {
        val trainDataList = nextTrain()
        var currentTrain = trainDataList[0] as Train
        var currentIndex = trainDataList[1] as Int
        var bDirection = trainDataList[2] as Boolean

        stations[currentIndex].trainB = null
        var trainAdvanced = 0
        while (trainAdvanced < (numTrains - 1)) {
            if (bDirection) {
                stations[currentIndex].trainB = null
            } else {
                stations[currentIndex].trainA = null
            }
            val prevStationId = stations[currentIndex].stationId
            val prevDirection = if (bDirection) "b" else "a"

            val advPair = getNextIndex(currentIndex, bDirection, 1)
            bDirection = advPair.second
            currentIndex = advPair.first
            if (bDirection) {
                stations[currentIndex].arriveB(currentTrain, prevStationId, prevDirection)
            } else {
                stations[currentIndex].arriveA(currentTrain, prevStationId, prevDirection)
            }
            val move = if (bDirection) 1 else -1
            val moveResultList = nextTrain(currentIndex + move, bDirection)
            val nextTrain = moveResultList[0] as Train
            currentIndex = moveResultList[1] as Int
            bDirection = moveResultList[2] as Boolean

            currentTrain = nextTrain
            trainAdvanced += 1


        }
        // The last train departs the current station
        if (bDirection) {
            stations[currentIndex].trainB = null
        } else {
            stations[currentIndex].trainA = null
        }

        //  Advance last train to the next station
        val prevStationId = stations[currentIndex].stationId
        val prevDirection = if (bDirection) "b" else "a"
        val moveResultPair = getNextIndex(currentIndex, bDirection, 1)
        bDirection = moveResultPair.second
        currentIndex = moveResultPair.first

        if (bDirection) {
            stations[currentIndex].arriveB(currentTrain, prevStationId, prevDirection)
        } else {
            stations[currentIndex].arriveA(currentTrain, prevStationId, prevDirection)
        }


    }

    fun nextTrain(startIndex: Int = 0, bDirection: Boolean = true, stepSize: Int = 1): List<Any?> {
        var currentIndex: Int
        var bDir = bDirection
        if (bDir) {
            currentIndex = nextTrainB(startIndex, stepSize)
            if (currentIndex == -1) {
                currentIndex = nextTrainA(stations.size - 1, stepSize)
                bDir = false
            }
        } else {
            currentIndex = nextTrainA(startIndex, stepSize)
            if (currentIndex == -1) {
                currentIndex = nextTrainB(0, stepSize)
                bDir = true
            }
        }
        if (bDir) {
            return listOf(stations[currentIndex].trainB, currentIndex, true)
        }
        return listOf(stations[currentIndex].trainA, currentIndex, false)

    }

    fun nextTrainB(startIndex: Int, stepSize: Int): Int {
        for (i in startIndex..stations.size step stepSize) {
            if (stations[i].trainB != null) {
                return i
            }
        }
        return -1
    }

    fun nextTrainA(startIndex: Int, stepSize: Int): Int {
        for (i in stations.size downTo startIndex step stepSize) {
            if (stations[i].trainA != null) {
                return i
            }
        }
        return -1
    }

    fun close() {
        stations.forEach { it.close() }
    }
}