package com.ecommerce.analytics

import java.time.format.{DateTimeFormatter, ResolverStyle, TextStyle}
import java.time.{DayOfWeek, LocalDateTime}
import java.util.Locale

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

/** Structure renvoyée par l'UDF (devient un struct Spark, éclaté ensuite en colonnes simples). */
case class TimeFeaturesResult(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

/** UDF extractTimeFeatures (Membre B). */
object TimeFeatures {

  private val Formatter = DateTimeFormatter.ofPattern("uuuuMMddHHmmss").withResolverStyle(ResolverStyle.STRICT)

  /** Fonction pure (testable sans Spark). Renvoie None pour une chaîne nulle, vide ou mal formée. */
  def compute(ts: String, workStart: Int = 9, workEnd: Int = 17): Option[TimeFeaturesResult] = {
    if (ts == null) None
    else {
      val s = ts.trim
      if (s.length != 14 || !s.forall(_.isDigit)) None
      else Try(LocalDateTime.parse(s, Formatter)).toOption.map { dt =>
        val h = dt.getHour
        val dow = dt.getDayOfWeek
        val period =
          if (h >= 6 && h < 12) "Morning"
          else if (h >= 12 && h < 18) "Afternoon"
          else if (h >= 18 && h < 22) "Evening"
          else "Night"
        TimeFeaturesResult(
          hour = h,
          day_of_week = dow.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          month = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          is_weekend = if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) 1 else 0,
          day_period = period,
          is_working_hours = if (h >= workStart && h <= workEnd) 1 else 0
        )
      }
    }
  }

  /** UDF Spark : une entrée invalide donne un struct null, jamais une exception. */
  def extractTimeFeaturesUdf(workStart: Int, workEnd: Int): UserDefinedFunction =
    udf((ts: String) => compute(ts, workStart, workEnd).orNull)
}
