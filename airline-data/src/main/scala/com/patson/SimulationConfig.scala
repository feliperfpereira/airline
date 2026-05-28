package com.patson

object SimulationConfig {
  private def readBoolean(propertyKey: String, envKey: String, default: Boolean): Boolean = {
    sys.props.get(propertyKey)
      .orElse(sys.env.get(envKey))
      .flatMap { v =>
        v.trim.toLowerCase match {
          case "1" | "true" | "yes" | "y" | "on" => Some(true)
          case "0" | "false" | "no" | "n" | "off" => Some(false)
          case _ => None
        }
      }
      .getOrElse(default)
  }

  val fastMode: Boolean = readBoolean("sim.fast", "SIM_FAST", default = false)
  val verbose: Boolean = readBoolean("sim.verbose", "SIM_VERBOSE", default = !fastMode)

  val refreshCachesEveryCycle: Boolean = readBoolean("sim.refreshCachesEveryCycle", "SIM_REFRESH_CACHES", default = true)

  val persistLinkStatistics: Boolean = readBoolean("sim.persistLinkStatistics", "SIM_PERSIST_LINK_STATISTICS", default = !fastMode)
  val persistAirportStatistics: Boolean = readBoolean("sim.persistAirportStatistics", "SIM_PERSIST_AIRPORT_STATISTICS", default = true)
  val persistConsumptionHistory: Boolean = readBoolean("sim.persistConsumptionHistory", "SIM_PERSIST_CONSUMPTION_HISTORY", default = !fastMode)
  val persistMissedDemandSnapshot: Boolean = readBoolean("sim.persistMissedDemandSnapshot", "SIM_PERSIST_MISSED_DEMAND", default = !fastMode)
  val persistCountryMarketShare: Boolean = readBoolean("sim.persistCountryMarketShare", "SIM_PERSIST_COUNTRY_MARKET_SHARE", default = !fastMode)
  val persistOlympicsStats: Boolean = readBoolean("sim.persistOlympicsStats", "SIM_PERSIST_OLYMPICS_STATS", default = !fastMode)

  val bookingOverbookingCheck: Boolean = readBoolean("sim.bookingOverbookingCheck", "SIM_BOOKING_OVERBOOKING_CHECK", default = !fastMode)

  // JFR: set SIM_CAPTURE_JFR=true to dump a cycle-N.jfr to recordings/ via Java Flight Recorder
  val captureJfr: Boolean = readBoolean("sim.captureJfr", "SIM_CAPTURE_JFR", default = false)

  // Watchdog: dump all thread stacks if any simulation phase exceeds this many seconds
  val watchdogEnabled: Boolean = readBoolean("sim.watchdogEnabled", "SIM_WATCHDOG_ENABLED", default = true)
  val watchdogThresholdSeconds: Int = sys.props.get("sim.watchdogThresholdSeconds")
    .orElse(sys.env.get("SIM_WATCHDOG_THRESHOLD_SECONDS"))
    .flatMap(_.toIntOption)
    .getOrElse(120)
}
