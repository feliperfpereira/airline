package com.patson.data

import scala.util.Using

object SimulationPerformanceSource {

  case class SimulationPerformanceRecord(
    cycle: Int,
    totalSeconds: Int,
    phaseTimingsJson: String,
    cores: Int,
    overbookingOk: Boolean,
    overbookingDetail: String
  )

  def save(cycle: Int, totalSeconds: Int, phaseTimings: Map[String, Long], cores: Int, overbookingOk: Boolean, overbookingDetail: String): Unit = {
    val phaseJson = phaseTimings.map { case (k, v) => s""""$k":$v""" }.mkString("{", ",", "}")
    Using.resource(Meta.getConnection()) { conn =>
      Using.resource(conn.prepareStatement(
        "REPLACE INTO simulation_performance (cycle, total_seconds, phase_timings, cores, overbooking_ok, overbooking_detail) VALUES (?,?,?,?,?,?)"
      )) { ps =>
        ps.setInt(1, cycle)
        ps.setInt(2, totalSeconds)
        ps.setString(3, phaseJson)
        ps.setInt(4, cores)
        ps.setBoolean(5, overbookingOk)
        ps.setString(6, overbookingDetail)
        ps.executeUpdate()
      }
    }
  }

  def loadRecent(limit: Int): List[SimulationPerformanceRecord] = {
    Using.resource(Meta.getConnection()) { conn =>
      Using.resource(conn.prepareStatement(
        "SELECT cycle, total_seconds, phase_timings, cores, overbooking_ok, overbooking_detail FROM simulation_performance ORDER BY cycle DESC LIMIT ?"
      )) { ps =>
        ps.setInt(1, limit)
        Using.resource(ps.executeQuery()) { rs =>
          val buf = scala.collection.mutable.ListBuffer[SimulationPerformanceRecord]()
          while (rs.next()) {
            buf += SimulationPerformanceRecord(
              cycle          = rs.getInt("cycle"),
              totalSeconds   = rs.getInt("total_seconds"),
              phaseTimingsJson = Option(rs.getString("phase_timings")).getOrElse("{}"),
              cores          = rs.getInt("cores"),
              overbookingOk  = rs.getBoolean("overbooking_ok"),
              overbookingDetail = Option(rs.getString("overbooking_detail")).getOrElse("")
            )
          }
          buf.toList
        }
      }
    }
  }
}
