package com.ecommerce.utils

import java.time.{Instant, ZoneId}
import scala.collection.mutable.ArrayBuffer

case class StepTiming(step: String, startMs: Long, endMs: Long) {
  def durationMs: Long = endMs - startMs
}

/** Journalise heure de début, heure de fin et durée de chaque étape (Q5.3 / Q6.2). */
class StepTimer {
  private val buffer = ArrayBuffer.empty[StepTiming]

  private def fmt(ms: Long): String =
    Instant.ofEpochMilli(ms).atZone(ZoneId.systemDefault()).toLocalTime.withNano(0).toString

  def time[T](step: String)(body: => T): T = {
    val start = System.currentTimeMillis()
    println(s"[ETAPE $step] début : ${fmt(start)}")
    try body
    finally {
      val end = System.currentTimeMillis()
      buffer += StepTiming(step, start, end)
      println(s"[ETAPE $step] fin : ${fmt(end)} - durée : ${end - start} ms")
    }
  }

  def timings: Seq[StepTiming] = buffer.toList
}
