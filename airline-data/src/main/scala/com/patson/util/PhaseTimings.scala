package com.patson.util

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

object PhaseTimings {
  private val store = new ConcurrentHashMap[String, Long]()

  def record(key: String, ms: Long): Unit = store.put(key, ms)

  /** Returns all recorded timings and resets the store. Call once per cycle. */
  def drain(): Map[String, Long] = {
    val result = store.asScala.toMap
    store.clear()
    result
  }
}
