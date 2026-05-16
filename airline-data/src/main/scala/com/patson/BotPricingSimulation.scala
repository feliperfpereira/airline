package com.patson

import com.patson.data.{AirlineSource, LinkSource}
import com.patson.model._
import com.typesafe.config.ConfigFactory

import scala.jdk.CollectionConverters._
import scala.collection.mutable

object BotPricingSimulation {

  private case class BotProfile(aggressiveness: Double, minPriceRatio: Double, maxPriceRatio: Double)

  private val DEFAULT_PROFILE = BotProfile(0.5, 0.6, 1.3)
  private val MAX_CHANGE_PER_CYCLE = 0.05 // max 5% change per cycle at aggressiveness=1.0

  def simulate(cycle: Int): Unit = {
    val profilesByName = loadBotProfiles()
    val botAirlines = AirlineSource.loadAirlinesByCriteria(List(("airline_type", NonPlayerAirline.id)))
    if (botAirlines.isEmpty) return

    val updatedLinks = mutable.ListBuffer[Link]()
    // Load all flight links once, group by (from, to) — eliminates per-pair DB round-trips
    val competitorsByPair: Map[(Int, Int), List[Link]] =
      LinkSource.loadAllFlightLinks().groupBy(l => (l.from.id, l.to.id))

    botAirlines.foreach { airline =>
      val profile = profilesByName.getOrElse(airline.name, DEFAULT_PROFILE)
      if (profile.aggressiveness > 0) {
        val links = LinkSource.loadFlightLinksByAirlineId(airline.id)
        if (links.nonEmpty) {
          val consumptionByLinkId: Map[Int, LinkConsumptionDetails] =
            LinkSource.loadLinkConsumptionsByAirline(airline.id, 1)
              .groupBy(_.link.id)
              .map { case (id, list) => id -> list.head }

          links.foreach { link =>
            val competitors = competitorsByPair.getOrElse((link.from.id, link.to.id), Nil)
              .filter(_.airline.id != airline.id)
            val maybeConsumption = consumptionByLinkId.get(link.id)
            val newPrice = computeNewPrice(link, maybeConsumption, competitors, profile)
            if (newPrice != link.price) {
              updatedLinks += link.copy(price = newPrice)
            }
          }
        }
      }
    }

    if (updatedLinks.nonEmpty) {
      println(s"[BotPricing] cycle $cycle: adjusting ${updatedLinks.size} bot link prices")
      LinkSource.updateLinks(updatedLinks.toList)
    }
  }

  private def computeNewPrice(
    link: Link,
    consumption: Option[LinkConsumptionDetails],
    competitors: List[Link],
    profile: BotProfile
  ): LinkClassValues = {

    val loadFactor = consumption.map { c =>
      val cap = c.link.capacity.total
      if (cap > 0) c.link.soldSeats.total.toDouble / cap else 0.0
    }

    val profit = consumption.map(_.profit).getOrElse(0)

    val signals = mutable.ListBuffer[Double]()

    loadFactor.foreach { lf =>
      if (lf < 0.5) {
        signals += -1.0 * (0.5 - lf) / 0.5 // below 50% LF: lower price
      } else if (lf > 0.85) {
        signals += 1.0 * (lf - 0.85) / 0.15 // above 85% LF: raise price
      }

      if (profit < 0) {
        if (lf > 0.7) {
          signals += 0.8 // selling well but losing money: price too low, raise
        } else if (lf < 0.3) {
          signals += 0.5 // dying route: retreat toward standard price
        }
      }

      if (competitors.nonEmpty && lf < 0.7) {
        val myEconomy = link.price(ECONOMY)
        val minCompEconomy = competitors.map(_.price(ECONOMY)).filter(_ > 0).minOption
        minCompEconomy.foreach { minComp =>
          if (minComp < myEconomy * 0.95) {
            signals += -0.5 // a cheaper competitor exists and we have room: undercut
          }
        }
      }
    }

    val avgSignal = if (signals.isEmpty) 0.0 else signals.sum / signals.size
    val multiplier = 1.0 + avgSignal * MAX_CHANGE_PER_CYCLE * profile.aggressiveness

    val flightCategory = Computation.getFlightCategory(link.from, link.to)
    val standardE = Pricing.computeStandardPrice(link.distance, flightCategory, ECONOMY, PassengerType.TRAVELER, link.from.baseIncome)
    val standardB = Pricing.computeStandardPrice(link.distance, flightCategory, BUSINESS, PassengerType.BUSINESS, link.from.baseIncome)
    val standardF = Pricing.computeStandardPrice(link.distance, flightCategory, FIRST, PassengerType.BUSINESS, link.from.baseIncome)

    def adjustPrice(current: Int, standard: Int): Int = {
      if (standard <= 0 || current <= 0) return current
      val raw = (current * multiplier).toInt
      val floor = (standard * profile.minPriceRatio).toInt
      val ceiling = (standard * profile.maxPriceRatio).toInt
      Math.max(floor, Math.min(ceiling, raw))
    }

    LinkClassValues(
      adjustPrice(link.price(ECONOMY), standardE),
      adjustPrice(link.price(BUSINESS), standardB),
      adjustPrice(link.price(FIRST), standardF)
    )
  }

  private def loadBotProfiles(): Map[String, BotProfile] =
    try {
      val config = ConfigFactory.load("bots")
      if (!config.hasPath("custom-bots")) return Map.empty
      config.getConfigList("custom-bots").asScala.map { bc =>
        val name = bc.getString("name")
        val aggressiveness = if (bc.hasPath("pricingAggressiveness")) bc.getDouble("pricingAggressiveness") else DEFAULT_PROFILE.aggressiveness
        val minRatio = if (bc.hasPath("minPriceRatio")) bc.getDouble("minPriceRatio") else DEFAULT_PROFILE.minPriceRatio
        val maxRatio = if (bc.hasPath("maxPriceRatio")) bc.getDouble("maxPriceRatio") else DEFAULT_PROFILE.maxPriceRatio
        name -> BotProfile(aggressiveness, minRatio, maxRatio)
      }.toMap
    } catch {
      case e: Exception =>
        println(s"[BotPricing] Could not load bots.conf: ${e.getMessage}")
        Map.empty
    }
}
