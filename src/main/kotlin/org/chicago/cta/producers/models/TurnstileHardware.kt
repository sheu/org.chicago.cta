package org.chicago.cta.producers.models

import org.chicago.cta.consumers.models.Station
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.math.floor
import kotlin.math.max

class TurnstileHardware constructor(private val stationId: Int, private val pathToRidershipDataDir: String, ) {
    private val weeklyRidership: Int
    private val saturdayRidership: Int
    private val sundaryRidership: Int
    private val metricsDf: SeedDf
    init {
        loadData()
        metricsDf = seedDf.find { it.stationId == stationId }!!
        weeklyRidership = metricsDf.avgWeekDayRides.toInt()
        saturdayRidership = metricsDf.avgSaturdayRides.toInt()
        sundaryRidership = metricsDf.avgSundayHolidayRides.toInt()
    }


   data class CurveDf(val hour: Int, val ridershipRation: Double)
    data class SeedDf(val stationId: Int,
                      val stationName: String,
                      val monthBeginning: String,
                      val avgWeekDayRides: Double,
                      val avgSaturdayRides: Double,
                      val avgSundayHolidayRides: Double,
                    val monthlyTotal: Int)

    private fun loadData() {
        if(curveDf.isEmpty())
        curveDf = Files.readAllLines(Paths.get("$pathToRidershipDataDir/ridership_curve.csv")).map { it.split(",") }.map { CurveDf(it[0].toInt(), it[1].toDouble()) }

        if(seedDf.isEmpty()) {
            seedDf = Files.readAllLines(Paths.get("$pathToRidershipDataDir/ridership_seed.csv"))
                    .map { it.split(",") }
                    .map { SeedDf(it[0].toInt(), it[1], it[2], it[3].toDouble(), it[4].toDouble(), it[5].toDouble(), it[6].toInt()) }
        }
    }

    companion object {
        var curveDf: List<CurveDf>  = emptyList()
        var seedDf: List<SeedDf> = emptyList()
    }

    fun getEntries(calendar: Calendar, totalSeconds: Long ): Double {
        val hourCurve = curveDf.find { it.hour == calendar.get(Calendar.HOUR) }!!
        val  ratio = hourCurve.ridershipRation
        val totalSteps  = (60.0/ (60.0 / totalSeconds))
        val numRiders = when (calendar.get(Calendar.DAY_OF_WEEK)) {
            in 0..4 -> {
                weeklyRidership
            }
            6 -> {
                saturdayRidership
            }
            else -> {
                sundaryRidership
            }
        }
        val numEntries = floor(numRiders * ratio / totalSteps)
        return max(numEntries + (-5..5).shuffled().first(), 0.0)
    }





}