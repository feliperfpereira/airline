package com.patson

import java.util.concurrent.TimeUnit
import org.apache.pekko.actor.Props
import org.apache.pekko.actor.Actor
import com.patson.data._
import com.patson.stream.{CycleCompleted, CycleStart, SimulationEventStream}
import com.patson.model.CountryAirlineTitle
import com.patson.util.{AirlineCache, AirplaneOwnershipCache, AirportCache, AirportStatisticsCache}
import scala.collection.mutable

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object MainSimulation extends App {
  val CYCLE_DURATION : Int = 60 * 29
  var currentWeek: Int = 0

  mainFlow

  def mainFlow() = {
    val actor = SimulationEventStream.system.actorOf(Props[MainSimulationActor], "mainSimulationActor")
    Await.result(SimulationEventStream.system.whenTerminated, Duration.Inf)
  }

  def initializeCaches() = {
    println("Initializing caches...")
    val startTime = System.currentTimeMillis()
    AirportCache.getAllAirports(true)
    val endTime = System.currentTimeMillis()
    println(s"Airport cache initialization completed in ${endTime - startTime}ms")
  }

  def invalidateCaches() = {
    AirlineCache.invalidateAll()
    AirportCache.invalidateAll()
    AirportStatisticsCache.invalidateAll()
    AirplaneOwnershipCache.invalidateAll()
    CountryAirlineTitle.invalidateAll()
  }

  def startCycle(cycle : Int) = {
    val cycleStartTime = System.currentTimeMillis()
    val phaseTimings = mutable.LinkedHashMap[String, Long]()

    def timed[T](name: String)(block: => T): T = {
      val t0 = System.currentTimeMillis()
      val result = block
      phaseTimings(name) = System.currentTimeMillis() - t0
      result
    }

    println("cycle " + cycle + " starting!")
    if (cycle == 1) { //initialize it
      OilSimulation.simulate(1)
      LoanInterestRateSimulation.simulate(1)
    }

    SimulationEventStream.publish(CycleStart(cycle, cycleStartTime), None)
    if (SimulationConfig.refreshCachesEveryCycle) {
      invalidateCaches()
      initializeCaches()
    }

    timed("userSimulation") { UserSimulation.simulate(cycle) }
    println("Event simulation")
    timed("eventSimulation") { EventSimulation.simulate(cycle) }
    println("Event simulation done")

    println("Bot pricing simulation")
    timed("botPricingSimulation") { BotPricingSimulation.simulate(cycle) }
    println("Bot pricing simulation done")

    println("Link simulation starting")
    val (flightLinkResult, loungeResult, linkRidershipDetails, paxStatsByAirlineId, overbookingOk) =
      timed("linkSimulation") { LinkSimulation.linkSimulation(cycle) }
    println("Link simulation done")

    println("Airport simulation")
    val airportChampionInfo = timed("airportSimulation") { AirportSimulation.airportSimulation(cycle, linkRidershipDetails) }
    println("Airport simulation done")

    println("Alliance simulation")
    timed("allianceSimulation") { AllianceSimulation.simulate(flightLinkResult, loungeResult, paxStatsByAirlineId, airportChampionInfo, cycle) }
    println("Alliance simulation done")

    println("Airplane simulation")
    val airplanes = timed("airplaneSimulation") { AirplaneSimulation.airplaneSimulation(cycle) }
    println("Airplane simulation done")

    println("Airline simulation")
    timed("airlineSimulation") { AirlineSimulation.airlineSimulation(cycle, flightLinkResult, loungeResult, airplanes, paxStatsByAirlineId) }
    println("Airline simulation done")

    println("Airplane model simulation")
    timed("airplaneModelSimulation") { AirplaneModelSimulation.simulate(cycle) }
    println("Airplane model simulation done")

    //purge history
    println("Purging link history")
    ChangeHistorySource.deleteLinkChangeByCriteria(List(("cycle", "<", cycle - 400)))

    //purge airline modifier
    println("Purging airline modifier")
    AirlineSource.deleteAirlineModifierByExpiry(cycle)

    val cycleEnd = System.currentTimeMillis()
    val totalSeconds = ((cycleEnd - cycleStartTime) / 1000).toInt

    println(">>>>> cycle " + cycle + " spent " + totalSeconds + " secs")

    try {
      SimulationPerformanceSource.save(
        cycle          = cycle,
        totalSeconds   = totalSeconds,
        phaseTimings   = phaseTimings.toMap,
        cores          = Runtime.getRuntime.availableProcessors,
        overbookingOk  = overbookingOk,
        overbookingDetail = ""
      )
    } catch {
      case e: Exception => println(s"Failed to save simulation performance record: ${e.getMessage}")
    }

    cycleEnd
  }

  /**
    * Things to be done after cycle ticked. These should be relatively short operations (data reconciliation etc)
    * @param currentCycle
    */
  def postCycle(currentCycle : Int) = {
    println("Oil simulation")
    OilSimulation.simulate(currentCycle)
    println("Loan simulation")
    LoanInterestRateSimulation.simulate(currentCycle)
    println("Add action points")
    ManagerSimulation.simulate(currentCycle)
    println("Post cycle link simulation")
    LinkSimulation.simulatePostCycle(currentCycle)

    println(s"Post cycle done $currentCycle")
  }

  // Actor Messages
  case object ExecuteProcessing
  case object BroadcastAndAdvance
  case object AdvanceOnce
  case class SetAutoAdvance(enabled: Boolean, delayMs: Long)

  class MainSimulationActor extends Actor {
    val POST_SIM_REST_MS = 1000L  // brief rest between sim completion and broadcast

    var currentWeek = CycleSource.loadCycle()
    var autoAdvance: Boolean = false
    var autoAdvanceDelayMs: Long = 1000L
    var pendingAdvance: Boolean = false

    def receive = {
      case AdvanceOnce =>
        if (status == SimulationStatus.IN_PROGRESS) {
          pendingAdvance = true
        } else {
          context.system.scheduler.scheduleOnce(Duration.Zero, self, ExecuteProcessing)
        }

      case SetAutoAdvance(enabled, delayMs) =>
        val wasIdle = status == SimulationStatus.WAITING_CYCLE_START && !autoAdvance
        autoAdvance = enabled
        autoAdvanceDelayMs = delayMs
        if (enabled && wasIdle) {
          context.system.scheduler.scheduleOnce(Duration.Zero, self, ExecuteProcessing)
        }

      case ExecuteProcessing =>
        status = SimulationStatus.IN_PROGRESS
        try {
          startCycle(currentWeek)
          currentWeek += 1
          CycleSource.setCycle(currentWeek)
          postCycle(currentWeek)
          context.system.scheduler.scheduleOnce(Duration(POST_SIM_REST_MS, TimeUnit.MILLISECONDS), self, BroadcastAndAdvance)
        } catch {
          case e : Exception =>
            println(s"!!!!!!! Cycle $currentWeek failed with exception: ${e.getClass.getName}: ${e.getMessage}. Retrying in 60s.")
            e.printStackTrace()
            status = SimulationStatus.WAITING_CYCLE_START
            context.system.scheduler.scheduleOnce(Duration(60, TimeUnit.SECONDS), self, ExecuteProcessing)
        }

      case BroadcastAndAdvance =>
        val endTime = System.currentTimeMillis()
        println("Publish Cycle Complete message")
        SimulationEventStream.publish(CycleCompleted(currentWeek - 1, endTime), None)
        status = SimulationStatus.WAITING_CYCLE_START
        if (pendingAdvance) {
          pendingAdvance = false
          context.system.scheduler.scheduleOnce(Duration.Zero, self, ExecuteProcessing)
        } else if (autoAdvance) {
          context.system.scheduler.scheduleOnce(Duration(autoAdvanceDelayMs, TimeUnit.MILLISECONDS), self, ExecuteProcessing)
        }
    }
  }

  var status : SimulationStatus.Value = SimulationStatus.WAITING_CYCLE_START
  object SimulationStatus extends Enumeration {
    type ManagerTaskType = Value
    val IN_PROGRESS, WAITING_CYCLE_START = Value
  }

}
