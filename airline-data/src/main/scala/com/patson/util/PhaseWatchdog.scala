package com.patson.util

import java.io.{File, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

/**
 * Detects hung phases and dumps all thread stacks to recordings/hang-*.txt.
 * Does not interrupt the phase — only captures evidence.
 *
 * Usage:
 *   val wd = new PhaseWatchdog(cycle, thresholdSeconds = 120)
 *   val result = wd.watch("myPhase") { expensiveWork() }
 *   wd.shutdown()
 */
class PhaseWatchdog(cycle: Int, thresholdSeconds: Int = 120) {
  private val scheduler = Executors.newSingleThreadScheduledExecutor { r =>
    val t = new Thread(r, "phase-watchdog")
    t.setDaemon(true)
    t
  }
  private val triggered  = new AtomicBoolean(false)
  private val fmt        = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")

  def watch[T](name: String)(body: => T): T = {
    triggered.set(false)
    val start   = System.currentTimeMillis()
    val future: ScheduledFuture[_] = scheduler.schedule(
      new Runnable { def run(): Unit = if (!triggered.get()) dump(name, start) },
      thresholdSeconds, TimeUnit.SECONDS
    )
    try body finally {
      triggered.set(true)
      future.cancel(false)
    }
  }

  def shutdown(): Unit = scheduler.shutdownNow()

  private def dump(name: String, start: Long): Unit = {
    val elapsed = (System.currentTimeMillis() - start) / 1000
    val ts      = fmt.format(LocalDateTime.now())
    val dir     = new File("recordings")
    dir.mkdirs()
    val file = new File(dir, s"hang-cycle${cycle}-phase${name}-${ts}.txt")
    try {
      val fw = new FileWriter(file)
      try {
        fw.write(s"HANG: cycle=$cycle phase=$name elapsed=${elapsed}s threshold=${thresholdSeconds}s ts=$ts\n\n")
        Thread.getAllStackTraces.forEach { (t, frames) =>
          fw.write(s"Thread: ${t.getName} state=${t.getState}\n")
          frames.foreach(f => fw.write(s"  at $f\n"))
          fw.write("\n")
        }
      } finally fw.close()
      println(s"[Watchdog] HANG in phase '$name' (${elapsed}s >= ${thresholdSeconds}s). Dump: ${file.getAbsolutePath}")
    } catch {
      case e: Exception => println(s"[Watchdog] Failed to write dump: ${e.getMessage}")
    }
  }
}
