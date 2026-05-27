package com.patson.data
import com.patson.data.Constants._
import com.patson.model._
import com.patson.util.AirportCache

import scala.collection.mutable.ListBuffer
import scala.util.Using


object ConsumptionHistorySource {
  var MAX_CONSUMPTION_HISTORY_WEEK = 12

  val updateConsumptions = (consumptions : Map[(PassengerGroup, Airport, Route), Int]) => {
    import scala.concurrent._
    import scala.concurrent.duration._
    import java.util.concurrent.{Executors, TimeUnit => JTimeUnit}

    // Phase 1: Create temp tables (DDL, separate connection)
    Using.resource(Meta.getConnection()) { connection =>
      Using.resource(connection.createStatement()) { ddlStatement =>
        ddlStatement.executeUpdate("DROP TABLE IF EXISTS " + PASSENGER_ROUTE_HISTORY_TABLE_TEMP)
        ddlStatement.executeUpdate("DROP TABLE IF EXISTS " + PASSENGER_LINK_HISTORY_TABLE_TEMP)
        ddlStatement.executeUpdate("CREATE TABLE " + PASSENGER_ROUTE_HISTORY_TABLE_TEMP + " LIKE " + PASSENGER_ROUTE_HISTORY_TABLE)
        ddlStatement.executeUpdate("CREATE TABLE " + PASSENGER_LINK_HISTORY_TABLE_TEMP + " LIKE " + PASSENGER_LINK_HISTORY_TABLE)
      }
    }

    // Phase 2: Parallel insert — each thread gets a disjoint route_id range
    val consumptionSeq = consumptions.toIndexedSeq
    val totalRoutes = consumptionSeq.size
    if (totalRoutes > 0) {
      val nThreads = math.min(Runtime.getRuntime.availableProcessors(), 8)
      val chunkSize = math.ceil(totalRoutes.toDouble / nThreads).toInt
      val batchSize = 5000
      val executor = Executors.newFixedThreadPool(nThreads)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

      val futures: List[Future[Unit]] = consumptionSeq.grouped(chunkSize).zipWithIndex.map { case (chunk, threadIdx) =>
        val startRouteId = threadIdx * chunkSize + 1
        Future {
          Using.resource(Meta.getConnection()) { conn =>
            conn.setAutoCommit(false)
            Using.resource(conn.prepareStatement("INSERT INTO " + PASSENGER_ROUTE_HISTORY_TABLE_TEMP +
              " (route_id, passenger_count, home_country, home_airport, destination_airport, passenger_type, preference_type, preferred_link_class, route_cost) VALUES(?,?,?,?,?,?,?,?,?)")) { routeStmt =>
              Using.resource(conn.prepareStatement("INSERT INTO " + PASSENGER_LINK_HISTORY_TABLE_TEMP +
                " (route_id, link, link_class, inverted, cost, satisfaction) VALUES(?,?,?,?,?,?)")) { linkStmt =>
                var localBatchCount = 0
                chunk.zipWithIndex.foreach { case (((passengerGroup, _, route), passengerCount), idx) =>
                  val routeId = startRouteId + idx
                  val preferredLinkClass = passengerGroup.preference.preferredLinkClass
                  routeStmt.setInt(1, routeId)
                  routeStmt.setInt(2, passengerCount)
                  routeStmt.setString(3, passengerGroup.fromAirport.countryCode)
                  routeStmt.setInt(4, passengerGroup.fromAirport.id)
                  routeStmt.setInt(5, route.links.last.to.id)
                  routeStmt.setInt(6, passengerGroup.passengerType.id)
                  routeStmt.setInt(7, passengerGroup.preference.getPreferenceType.id)
                  routeStmt.setString(8, preferredLinkClass.code)
                  routeStmt.setInt(9, route.totalCost.toInt)
                  routeStmt.addBatch()
                  route.links.foreach { lc =>
                    val satisfaction = Computation.computePassengerSatisfaction(
                      lc.cost.toInt, lc.link.standardPrice(preferredLinkClass, passengerGroup.passengerType),
                      lc.link.getLoadFactor, lc.link.getDelayRatio)
                    linkStmt.setInt(1, routeId)
                    linkStmt.setInt(2, lc.link.id)
                    linkStmt.setString(3, lc.linkClass.code)
                    linkStmt.setBoolean(4, lc.inverted)
                    linkStmt.setDouble(5, lc.cost)
                    linkStmt.setDouble(6, satisfaction)
                    linkStmt.addBatch()
                  }
                  localBatchCount += 1
                  if (localBatchCount % batchSize == 0) {
                    routeStmt.executeBatch()
                    linkStmt.executeBatch()
                  }
                }
                routeStmt.executeBatch()
                linkStmt.executeBatch()
              }
            }
            conn.commit()
          }
        }
      }.toList

      try {
        Await.result(Future.sequence(futures), Duration(120, JTimeUnit.SECONDS))
      } finally {
        executor.shutdown()
      }
    }

    // Phase 2: rotate tables using a fresh connection. DDL-only, completes in seconds.
    // Uses one metadata query per table family to find existing numbered tables, then
    // issues a single atomic RENAME TABLE instead of ~170 serial DDL statements.
    println("Rotating tables")
    Using.resource(Meta.getConnection()) { connection =>
      Using.resource(connection.createStatement()) { rotateStatement =>

        def batchRotate(baseTable: String): Unit = {
          // Find which numbered slots (1..MAX-1) currently exist for this table family.
          val prefix = baseTable + "_"
          val existingNums = Using.resource(connection.prepareStatement(
            "SELECT table_name FROM information_schema.tables " +
            "WHERE table_schema = DATABASE() AND table_name LIKE ? " +
            "AND table_name REGEXP ?"
          )) { ps =>
            ps.setString(1, prefix + "%")
            ps.setString(2, s"^${java.util.regex.Pattern.quote(baseTable)}_[0-9]+$$")
            Using.resource(ps.executeQuery()) { rs =>
              val nums = scala.collection.mutable.ListBuffer[Int]()
              while (rs.next()) {
                val tname = rs.getString(1)
                val numStr = tname.substring(prefix.length)
                scala.util.Try(numStr.toInt).foreach { n =>
                  if (n >= 1 && n < MAX_CONSUMPTION_HISTORY_WEEK) nums += n
                }
              }
              nums.toList.sorted(Ordering[Int].reverse) // descending: 29, 28, ..., 1
            }
          }

          if (existingNums.nonEmpty) {
            // Drop the overflow slot to free it for the highest existing table.
            rotateStatement.executeUpdate(s"DROP TABLE IF EXISTS ${baseTable}_${MAX_CONSUMPTION_HISTORY_WEEK}")

            // Single RENAME TABLE: _N→_{N+1}, ..., _1→_2  (MySQL processes left-to-right,
            // so each source is free by the time the next rename needs it as a destination).
            val renamePairs = existingNums.map(n => s"${baseTable}_$n TO ${baseTable}_${n + 1}")
            rotateStatement.executeUpdate("RENAME TABLE " + renamePairs.mkString(", "))
          }
        }

        batchRotate(PASSENGER_ROUTE_HISTORY_TABLE)
        batchRotate(PASSENGER_LINK_HISTORY_TABLE)

        // Atomically swap base→_1 and temp→base in a single RENAME TABLE statement.
        // If temp doesn't exist the whole statement is reverted, leaving base intact.
        rotateStatement.executeUpdate(s"DROP TABLE IF EXISTS ${PASSENGER_ROUTE_HISTORY_TABLE}_1")
        rotateStatement.executeUpdate(s"DROP TABLE IF EXISTS ${PASSENGER_LINK_HISTORY_TABLE}_1")
        rotateStatement.executeUpdate(
          s"RENAME TABLE " +
          s"$PASSENGER_ROUTE_HISTORY_TABLE TO ${PASSENGER_ROUTE_HISTORY_TABLE}_1, " +
          s"$PASSENGER_ROUTE_HISTORY_TABLE_TEMP TO $PASSENGER_ROUTE_HISTORY_TABLE, " +
          s"$PASSENGER_LINK_HISTORY_TABLE TO ${PASSENGER_LINK_HISTORY_TABLE}_1, " +
          s"$PASSENGER_LINK_HISTORY_TABLE_TEMP TO $PASSENGER_LINK_HISTORY_TABLE"
        )
      }
      println("Finished rotating tables")
    }
  }

  case class TicketedDemandEntry(toAirportId: Int, passengerType: Int, preferenceType: Int, preferredLinkClass: String, passengerCount: Int, airlineIds: List[Int])

  /**
   * Top individual route journeys from a given origin airport by passenger count.
   * Groups by route_id to collect all airline IDs across every leg of each journey.
   */
  def loadTopConsumptionsByFromAirport(fromAirportId: Int, limit: Int): List[TicketedDemandEntry] = {
    Using.resource(Meta.getConnection()) { connection =>
      Using.resource(connection.prepareStatement(
        s"""WITH TopRoutes AS (
            SELECT route_id, destination_airport, passenger_type, preference_type,
                preferred_link_class, passenger_count
            FROM $PASSENGER_ROUTE_HISTORY_TABLE
            WHERE home_airport = ?
            ORDER BY passenger_count DESC
            LIMIT ?
        )
        SELECT tr.destination_airport, tr.passenger_type, tr.preference_type, tr.preferred_link_class, tr.passenger_count,
            GROUP_CONCAT(DISTINCT l.airline ORDER BY l.airline) AS airline_ids
        FROM TopRoutes tr
        LEFT JOIN $PASSENGER_LINK_HISTORY_TABLE plh ON tr.route_id = plh.route_id
        LEFT JOIN $LINK_TABLE l ON plh.link = l.id
        GROUP BY tr.route_id, tr.destination_airport, tr.passenger_type, tr.preference_type, tr.preferred_link_class, tr.passenger_count
        ORDER BY tr.passenger_count DESC""".stripMargin
      )) { stmt =>
        stmt.setInt(1, fromAirportId)
        stmt.setInt(2, limit)
        Using.resource(stmt.executeQuery()) { rs =>
          val result = scala.collection.mutable.ListBuffer[TicketedDemandEntry]()
          while (rs.next()) {
            val airlineIds = Option(rs.getString("airline_ids"))
              .map(_.split(",").toList.flatMap(s => scala.util.Try(s.trim.toInt).toOption))
              .getOrElse(List.empty)
            result += TicketedDemandEntry(
              toAirportId = rs.getInt("destination_airport"),
              passengerType = rs.getInt("passenger_type"),
              preferenceType = rs.getInt("preference_type"),
              preferredLinkClass = rs.getString("preferred_link_class"),
              passengerCount = rs.getInt("passenger_count"),
              airlineIds = airlineIds
            )
          }
          result.toList
        }
      }
    }
  }

  def deleteAllConsumptions() = {
    Using.resource(Meta.getConnection()) { connection =>
      Using.resource(connection.createStatement()) { stmt =>
        for (i <- MAX_CONSUMPTION_HISTORY_WEEK to 1 by -1) {
          val routeTableName = PASSENGER_ROUTE_HISTORY_TABLE + "_" + i
          val linkTableName = PASSENGER_LINK_HISTORY_TABLE + "_" + i
          stmt.executeUpdate(s"DROP TABLE IF EXISTS $routeTableName")
          stmt.executeUpdate(s"DROP TABLE IF EXISTS $linkTableName")
        }
        stmt.executeUpdate(s"TRUNCATE TABLE $PASSENGER_ROUTE_HISTORY_TABLE")
        stmt.executeUpdate(s"TRUNCATE TABLE $PASSENGER_LINK_HISTORY_TABLE")
      }
    }
  }

  /**
   * Research, Airport search pair
   *
   * @param fromAirportId
   * @param toAirportId
   * @return
   */
  def loadConsumptionsByAirportPair(fromAirportId : Int, toAirportId : Int) : Map[Route, (PassengerType.Value, Int)] = {
    case class LinkRow(linkId: Int, routeId: Int, cost: Int, linkClass: String, inverted: Boolean)

    Using.resource(Meta.getConnection()) { connection =>
      // Phase 1: load matching routes
      val routeQueryString = s"SELECT route_id, passenger_type, passenger_count, route_cost FROM $PASSENGER_ROUTE_HISTORY_TABLE WHERE home_airport = ? AND destination_airport = ?"
      val (routeConsumptions, routeIdsWithCosts) = Using.resource(connection.prepareStatement(routeQueryString)) { routePreparedStatement =>
        routePreparedStatement.setInt(1, fromAirportId)
        routePreparedStatement.setInt(2, toAirportId)
        Using.resource(routePreparedStatement.executeQuery()) { routeResultSet =>
          val routeConsumptions = new scala.collection.mutable.HashMap[Int, (PassengerType.Value, Int)]()
          val routeIdsWithCosts = scala.collection.mutable.ListBuffer[(Int, Int)]()
          while (routeResultSet.next()) {
            val routeId = routeResultSet.getInt("route_id")
            val passengerType = PassengerType.apply(routeResultSet.getInt("passenger_type"))
            val passengerCount = routeResultSet.getInt("passenger_count")
            routeIdsWithCosts += ((routeId, routeResultSet.getInt("route_cost")))
            routeConsumptions.put(routeId, (passengerType, passengerCount))
          }
          (routeConsumptions, routeIdsWithCosts)
        }
      }

      // Build dynamic IN clause for link query
      if (routeIdsWithCosts.isEmpty) {
        Map.empty
      } else {
        val linkQueryString = s"SELECT * FROM $PASSENGER_LINK_HISTORY_TABLE WHERE route_id IN (${routeIdsWithCosts.map(_ => "?").mkString(",")})"
        val linkRows = Using.resource(connection.prepareStatement(linkQueryString)) { linkPreparedStatement =>
          for (i <- routeIdsWithCosts.indices) {
            linkPreparedStatement.setInt(i + 1, routeIdsWithCosts(i)._1)
          }
          Using.resource(linkPreparedStatement.executeQuery()) { linkResultSet =>
            val rows = new ListBuffer[LinkRow]()
            while (linkResultSet.next()) {
              rows += LinkRow(linkResultSet.getInt("link"), linkResultSet.getInt("route_id"), linkResultSet.getInt("cost"), linkResultSet.getString("link_class"), linkResultSet.getBoolean("inverted"))
            }
            rows.toList
          }
        }

        val linkConsumptionById: Map[Int, LinkConsumptionDetails] = LinkSource.loadLinkConsumptionsByLinksId(linkRows.map(_.linkId).distinct.toList).map(entry => (entry.link.id, entry)).toMap

        // Reconstruct routes with link considerations
        val linkConsiderationsByRouteId = scala.collection.mutable.Map[Int, ListBuffer[LinkConsideration]]()
        linkRows.foreach { row =>
          linkConsumptionById.get(row.linkId).foreach { linkConsumption =>
            val linkConsideration = LinkConsideration.getExplicit(linkConsumption.link, row.cost, LinkClass.fromCode(row.linkClass), row.inverted)
            val existingConsiderationsForThisRoute = linkConsiderationsByRouteId.getOrElseUpdate(row.routeId, ListBuffer[LinkConsideration]())
            existingConsiderationsForThisRoute += linkConsideration
          }
        }

        val routeIdsCostsMap = routeIdsWithCosts.toMap
        val result: Map[Route, (PassengerType.Value, Int)] = linkConsiderationsByRouteId.view.map {
          case (routeId: Int, considerations: ListBuffer[LinkConsideration]) =>
            val sortedConsiderations = sortLinks(considerations.toList, fromAirportId)
            (Route(sortedConsiderations, routeIdsCostsMap(routeId), routeId), routeConsumptions(routeId))
        }.toMap

        println(s"Loaded ${result.size} routes for airport pair ${fromAirportId} and ${toAirportId})")

        result
      }
    }
  }

  def sortLinks(links: List[LinkConsideration], fromAirportId: Int): List[LinkConsideration] = {
    var currentAirportId = fromAirportId
    val sortedLinks = ListBuffer[LinkConsideration]()
    val remainingLinks = scala.collection.mutable.ListBuffer(links: _*)

    while (remainingLinks.nonEmpty) {
      remainingLinks.find(_.from.id == currentAirportId) match {
        case Some(nextLink) =>
          sortedLinks += nextLink
          remainingLinks -= nextLink
          currentAirportId = nextLink.to.id
        case None =>
          // Cannot find next link.
          // Maybe the start airport was wrong?
          // Or the route is disjoint.
          return links // Fallback to unsorted
      }
    }
    sortedLinks.toList
  }

  /**
   *
   *
   * @param linkId
   * @param cycle
   * @return
   */
  def loadRelatedConsumptionByLinkId(linkId: Int, cycle: Int): Map[Route, (PassengerType.Value, Int)] = {
    val cycleDelta = cycle - CycleSource.loadCycle()

    val linkTableName =
      if (cycleDelta >= 0) {
        PASSENGER_LINK_HISTORY_TABLE
      } else {
        PASSENGER_LINK_HISTORY_TABLE + "_" + (cycleDelta * -1)
      }

    val routeTableName =
      if (cycleDelta >= 0) {
        PASSENGER_ROUTE_HISTORY_TABLE
      } else {
        PASSENGER_ROUTE_HISTORY_TABLE + "_" + (cycleDelta * -1)
      }

    LinkSource.loadFlightLinkById(linkId, LinkSource.SIMPLE_LOAD) match {
      case Some(link) =>
        Using.resource(Meta.getConnection()) { connection =>
          if (Meta.isTableExist(connection, linkTableName) && Meta.isTableExist(connection, routeTableName)) {
            // Phase 1: find all route IDs that contain this link
            val relatedRouteIds = Using.resource(connection.prepareStatement(s"SELECT DISTINCT route_id FROM $linkTableName WHERE link = ?")) { linkPreparedStatement =>
              linkPreparedStatement.setInt(1, linkId)
              Using.resource(linkPreparedStatement.executeQuery()) { linkResultSet =>
                val ids = new ListBuffer[Int]()
                while (linkResultSet.next()) {
                  ids += linkResultSet.getInt("route_id")
                }
                ids.toList
              }
            }

            if (relatedRouteIds.isEmpty) {
              Map.empty
            } else {
              val inClause = relatedRouteIds.map(_ => "?").mkString(",")

              // Phase 2: get route-level data
              val (routeConsumptions, routeCosts) = Using.resource(connection.prepareStatement(s"SELECT route_id, passenger_type, passenger_count, route_cost FROM $routeTableName WHERE route_id IN ($inClause)")) { routePreparedStatement =>
                for (i <- relatedRouteIds.indices) {
                  routePreparedStatement.setInt(i + 1, relatedRouteIds(i))
                }
                Using.resource(routePreparedStatement.executeQuery()) { routeResultSet =>
                  val routeConsumptions = new scala.collection.mutable.HashMap[Int, (PassengerType.Value, Int)]()
                  val routeCosts = new scala.collection.mutable.HashMap[Int, Int]()
                  while (routeResultSet.next()) {
                    val routeId = routeResultSet.getInt("route_id")
                    val passengerType = PassengerType.apply(routeResultSet.getInt("passenger_type"))
                    val passengerCount = routeResultSet.getInt("passenger_count")
                    val routeCost = routeResultSet.getInt("route_cost")
                    routeConsumptions.put(routeId, (passengerType, passengerCount))
                    routeCosts.put(routeId, routeCost)
                  }
                  (routeConsumptions, routeCosts)
                }
              }

              // Phase 3: collect all related link IDs
              val relatedLinkIds = Using.resource(connection.prepareStatement(s"SELECT route_id, link FROM $linkTableName WHERE route_id IN ($inClause)")) { allLinksPreparedStatement =>
                for (i <- 0 until relatedRouteIds.size) {
                  allLinksPreparedStatement.setInt(i + 1, relatedRouteIds(i))
                }
                Using.resource(allLinksPreparedStatement.executeQuery()) { allLinksResultSet =>
                  val ids = scala.collection.mutable.HashSet[Int]()
                  while (allLinksResultSet.next()) {
                    ids += allLinksResultSet.getInt("link")
                  }
                  ids.toList
                }
              }
              val linkMap = LinkSource.loadLinksByIds(relatedLinkIds).map(link => (link.id, link)).toMap

              // Phase 4: reconstruct link considerations per route
              val linkConsiderationsByRouteId = Using.resource(connection.prepareStatement(s"SELECT * FROM $linkTableName WHERE route_id IN ($inClause)")) { linkDataPreparedStatement =>
                for (i <- 0 until relatedRouteIds.size) {
                  linkDataPreparedStatement.setInt(i + 1, relatedRouteIds(i))
                }
                Using.resource(linkDataPreparedStatement.executeQuery()) { linkDataResultSet =>
                  val considerationsByRouteId = scala.collection.mutable.Map[Int, ListBuffer[LinkConsideration]]()
                  while (linkDataResultSet.next()) {
                    val routeId = linkDataResultSet.getInt("route_id")
                    val relatedLinkId = linkDataResultSet.getInt("link")
                    val relatedLink = linkMap.getOrElse(relatedLinkId, Link.fromId(relatedLinkId))
                    val linkConsideration = LinkConsideration.getExplicit(relatedLink, linkDataResultSet.getDouble("cost"), LinkClass.fromCode(linkDataResultSet.getString("link_class")), linkDataResultSet.getBoolean("inverted"))
                    considerationsByRouteId.getOrElseUpdate(routeId, ListBuffer[LinkConsideration]()) += linkConsideration
                  }
                  considerationsByRouteId
                }
              }

              val result = linkConsiderationsByRouteId.map {
                case (routeId, considerations) => (Route(considerations.toList, routeCosts(routeId), routeId), routeConsumptions(routeId))
              }.toMap

              println("Loaded " + result.size + " routes related to link " + link)

              result
            }
          } else {
            Map.empty
          }
        }
      case None => Map.empty
    }
  }

  /**
   * Used in survey
   *
   * @param linkId
   * @return
   */
  def loadConsumptionByLinkId(linkId : Int) : List[LinkConsumptionHistory] = {
    LinkSource.loadFlightLinkById(linkId) match {
      case Some(link) =>
        Using.resource(Meta.getConnection()) { connection =>
          // Query from link history table with JOIN to route history table
          val queryString = s"""
            SELECT
              pl.route_id, pl.link_class, pl.inverted, pl.satisfaction,
              pr.passenger_type, pr.passenger_count, pr.home_airport, pr.destination_airport,
              pr.preference_type, pr.preferred_link_class, pr.route_cost
            FROM $PASSENGER_LINK_HISTORY_TABLE pl
            JOIN $PASSENGER_ROUTE_HISTORY_TABLE pr ON pl.route_id = pr.route_id
            WHERE pl.link = ?
          """
          Using.resource(connection.prepareStatement(queryString)) { preparedStatement =>
            preparedStatement.setInt(1, linkId)
            Using.resource(preparedStatement.executeQuery()) { resultSet =>
              val result = new ListBuffer[LinkConsumptionHistory]()
              while (resultSet.next()) {
                result += LinkConsumptionHistory(link = link,
                    passengerCount = resultSet.getInt("passenger_count"),
                    homeAirport = AirportCache.getAirport(resultSet.getInt("home_airport")).get,
                    destinationAirport = AirportCache.getAirport(resultSet.getInt("destination_airport")).get,
                    passengerType = PassengerType(resultSet.getInt("passenger_type")),
                    preferredLinkClass = LinkClass.fromCode(resultSet.getString("preferred_link_class")),
                    preferenceType = FlightPreferenceType(resultSet.getInt("preference_type")),
                    linkClass = LinkClass.fromCode(resultSet.getString("link_class")),
                    satisfaction = resultSet.getDouble("satisfaction"),
                    routeCost = resultSet.getInt("route_cost")
                )
              }
              result.toList
            }
          }
        }
      case None => List.empty
    }
  }
}
