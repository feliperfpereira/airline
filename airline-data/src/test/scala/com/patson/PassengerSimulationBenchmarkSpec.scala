package com.patson

import com.patson.model._
import com.patson.model.airplane.{AirplaneMaintenanceUtil, Model}
import com.patson.util.PhaseTimings
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.lang.management.ManagementFactory
import scala.collection.mutable
import scala.util.Random

/**
 * Macro-benchmarks for PassengerSimulation hot path.
 *
 * Does NOT fail on any assertion — only prints a performance table.
 * Run with: sbt "airline-data/testOnly *BenchmarkSpec"
 *
 * REQUIRES a live MySQL DB (same connection as the simulation), because
 * PassengerSimulation.passengerConsume reads alliance/modifier/specialization data
 * and writes one WorldStatistics record per run. If DB is unavailable the test
 * prints a warning and skips gracefully.
 *
 * Scale used here (~300 airports, ~1500 links, ~30k demand chunks) is intentionally
 * smaller than production (1649 airports, 6639 links, ~1.5M demand chunks) so the
 * suite completes in seconds. Increase AIRPORT_COUNT / LINK_COUNT / DEMAND_COUNT
 * to stress-test at production scale locally.
 */
class PassengerSimulationBenchmarkSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private val AIRPORT_COUNT = 300
  private val LINK_COUNT    = 1500
  private val DEMAND_COUNT  = 30_000
  private val WARM_RUNS     = 1
  private val MEASURE_RUNS  = 3

  private val rng = new Random(42L)

  override def beforeAll(): Unit = {
    AirplaneMaintenanceUtil.setTestFactor(Some(1))
  }
  override def afterAll(): Unit = {
    AirplaneMaintenanceUtil.setTestFactor(None)
  }

  // ── synthetic world ─────────────────────────────────────────────────────────

  private val airline = Airline("BenchAirline", id = 1)
  private val appeal  = AirlineAppeal(0)

  private val airports: IndexedSeq[Airport] = (1 to AIRPORT_COUNT).map { i =>
    val a = Airport(
      iata          = f"A$i%03d",
      icao          = f"B$i%04d",
      name          = s"Airport $i",
      latitude      = -60.0 + rng.nextDouble() * 120,
      longitude     = -180.0 + rng.nextDouble() * 360,
      countryCode   = "US",
      city          = s"City $i",
      zone          = "NA",
      size          = 3 + rng.nextInt(5),
      baseIncome    = 20_000 + rng.nextInt(60_000),
      basePopulation = 500_000 + rng.nextInt(5_000_000),
      id            = i
    )
    a.initAirlineAppeals(Map(airline.id -> appeal))
    a.initLounges(List.empty)
    a
  }

  // Link topology (from/to/distance) is fixed; seats are reset on each benchRun call.
  private case class LinkBlueprint(from: Airport, to: Airport, capacity: Int, dist: Int, id: Int)

  private val linkBlueprints: List[LinkBlueprint] = {
    val buf = mutable.ListBuffer[LinkBlueprint]()
    var lid = 1
    airports.tail.foreach { to =>
      val dist = Computation.calculateDistance(airports(0), to).toInt.max(200)
      buf += LinkBlueprint(airports(0), to, 150, dist, lid); lid += 1
    }
    val rng2 = new Random(99L)
    while (buf.size < LINK_COUNT) {
      val from = airports(rng2.nextInt(AIRPORT_COUNT))
      val to   = airports(rng2.nextInt(AIRPORT_COUNT))
      if (from.id != to.id) {
        val dist = Computation.calculateDistance(from, to).toInt.max(200)
        buf += LinkBlueprint(from, to, 100, dist, lid); lid += 1
      }
    }
    buf.toList
  }

  private def freshLinks(): List[Link] = linkBlueprints.map { bp =>
    Link(bp.from, bp.to, airline,
      price      = LinkClassValues.getInstance(200),
      distance   = bp.dist,
      capacity   = LinkClassValues.getInstance(bp.capacity),
      rawQuality = 50, duration = bp.dist / 10, frequency = 7, id = bp.id)
  }

  private val demand: List[(PassengerGroup, Airport, Int)] = {
    val buf = mutable.ListBuffer[(PassengerGroup, Airport, Int)]()
    while (buf.size < DEMAND_COUNT) {
      val from    = airports(rng.nextInt(AIRPORT_COUNT))
      val to      = airports(rng.nextInt(AIRPORT_COUNT))
      if (from.id != to.id) {
        val pref  = DealPreference(homeAirport = from, preferredLinkClass = ECONOMY, priceModifier = 1.0)
        val group = PassengerGroup(from, pref, PassengerType.BUSINESS)
        buf      += ((group, to, 10 + rng.nextInt(40)))
      }
    }
    buf.toList
  }

  // ── benchmark runner ─────────────────────────────────────────────────────────

  private def gcCount(): Long = {
    import scala.jdk.CollectionConverters._
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionCount).sum
  }

  private def benchRun(label: String)(body: => Unit): BenchResult = {
    // warm up
    (0 until WARM_RUNS).foreach(_ => body)
    // measure
    val times = (0 until MEASURE_RUNS).map { _ =>
      val gcBefore  = gcCount()
      val t0        = System.currentTimeMillis()
      body
      val elapsed   = System.currentTimeMillis() - t0
      val gcAfter   = gcCount()
      (elapsed, gcAfter - gcBefore)
    }
    val sortedMs = times.map(_._1).sorted
    val totalGc  = times.map(_._2).sum
    val p50 = sortedMs(sortedMs.length / 2)
    val p99 = sortedMs(sortedMs.length - 1)
    BenchResult(label, sortedMs.min, p50, p99, totalGc)
  }

  case class BenchResult(label: String, minMs: Long, p50Ms: Long, p99Ms: Long, gcRuns: Long)

  private def printTable(results: Seq[BenchResult]): Unit = {
    val header = f"${"Benchmark"}%-55s ${"min(ms)"}%9s ${"p50(ms)"}%9s ${"p99(ms)"}%9s ${"GC runs"}%10s"
    val sep    = "-" * header.length
    println("\n" + sep)
    println(header)
    println(sep)
    results.foreach { r =>
      println(f"${r.label}%-55s ${r.minMs}%9d ${r.p50Ms}%9d ${r.p99Ms}%9d ${r.gcRuns}%10d")
    }
    println(sep + "\n")
  }

  // ── benchmarks ───────────────────────────────────────────────────────────────

  "PassengerSimulation benchmarks" should {

    "B1: full passengerConsume pipeline" in {
      val result = try {
        val r = benchRun(s"passengerConsume ($AIRPORT_COUNT airports, $LINK_COUNT links, $DEMAND_COUNT demand)") {
          PhaseTimings.drain() // clear any residual
          PassengerSimulation.passengerConsume(demand, freshLinks())
        }
        printTable(Seq(r))
        val subTimings = PhaseTimings.drain()
        if (subTimings.nonEmpty) {
          println("Sub-phase breakdown (last run):")
          subTimings.toList.sortBy(_._1).foreach { case (k, v) => println(f"  $k%-60s $v%6d ms") }
          println()
        }
        true
      } catch {
        case e: Exception =>
          println(s"[BenchmarkSpec] B1 skipped — DB unavailable or init error: ${e.getClass.getSimpleName}: ${e.getMessage}")
          true
      }
      result shouldBe true
    }

    "B2: edgeBuilding and routeFinding per-loop breakdown" in {
      val result = try {
        PassengerSimulation.passengerConsume(demand, freshLinks())
        val t = PhaseTimings.drain()
        val edgeBuildEntries = t.filter(_._1.contains("edgeBuilding"))
        val rfEntries        = t.filter(_._1.contains("routeFinding"))
        if (edgeBuildEntries.nonEmpty) {
          println(s"B2 edgeBuilding per loop: ${edgeBuildEntries.toList.sortBy(_._1).map { case (k,v) => s"${k.split('.')(1)}=${v}ms" }.mkString(", ")}")
          println(s"B2 routeFinding per loop: ${rfEntries.toList.sortBy(_._1).map { case (k,v) => s"${k.split('.')(1)}=${v}ms" }.mkString(", ")}")
        } else {
          println("B2: no edgeBuilding timings found — ensure PhaseTimings instrumentation is active")
        }
        true
      } catch {
        case e: Exception =>
          println(s"[BenchmarkSpec] B2 skipped — ${e.getClass.getSimpleName}: ${e.getMessage}")
          true
      }
      result shouldBe true
    }

    "B3: lock contention ratio (lockedSection vs parallelBook)" in {
      val result = try {
        PassengerSimulation.passengerConsume(demand, freshLinks())
        val t = PhaseTimings.drain()
        val totalLock = t.filter(_._1.contains("lockedSection")).values.sum
        val totalRf   = t.filter(_._1.contains("routeFinding")).values.sum
        val totalBook = t.filter(_._1.contains("parallelBook")).values.sum
        if (totalBook > 0) {
          println(s"B3 lock contention summary (all loops combined):")
          println(f"  routeFinding total:  $totalRf%6d ms")
          println(f"  lockedSection total: $totalLock%6d ms")
          println(f"  parallelBook total:  $totalBook%6d ms")
          println(f"  lock %%:  ${100.0 * totalLock / totalBook}%.1f%%  routeFinding %%: ${100.0 * totalRf / totalBook}%.1f%%")
        } else {
          println("B3: no parallelBook timings found")
        }
        true
      } catch {
        case e: Exception =>
          println(s"[BenchmarkSpec] B3 skipped — ${e.getClass.getSimpleName}: ${e.getMessage}")
          true
      }
      result shouldBe true
    }

    "B4: synchronizedList vs ConcurrentLinkedQueue throughput" in {
      import java.util.{Collections, ArrayList}
      import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors}

      val THREADS = Runtime.getRuntime.availableProcessors
      val OPS_PER_THREAD = 50_000

      def measure(name: String)(addFn: Int => Unit): Long = {
        val latch  = new CountDownLatch(THREADS)
        val pool   = Executors.newFixedThreadPool(THREADS)
        val start  = System.currentTimeMillis()
        (0 until THREADS).foreach { _ =>
          pool.submit(new Runnable {
            def run(): Unit = {
              (0 until OPS_PER_THREAD).foreach(addFn)
              latch.countDown()
            }
          })
        }
        latch.await()
        pool.shutdown()
        val elapsed = System.currentTimeMillis() - start
        println(f"  B4 $name%-35s threads=$THREADS ops/thread=$OPS_PER_THREAD elapsed=${elapsed}ms")
        elapsed
      }

      val syncList = Collections.synchronizedList(new ArrayList[Int]())
      val concQueue = new ConcurrentLinkedQueue[Int]()

      val syncMs  = measure("Collections.synchronizedList.add")(i => syncList.add(i))
      val queueMs = measure("ConcurrentLinkedQueue.offer")(i => concQueue.offer(i))

      println(f"  speedup: ${syncMs.toDouble / queueMs.toDouble}%.2fx (ConcurrentLinkedQueue vs synchronizedList)")
      succeed
    }
  }
}
