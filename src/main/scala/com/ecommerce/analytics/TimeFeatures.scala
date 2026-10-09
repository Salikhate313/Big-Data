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

  // « uuuu » + STRICT : une date impossible (31 février, mois 13) est refusée au lieu d'être corrigée
  private val Formatter = DateTimeFormatter.ofPattern("uuuuMMddHHmmss").withResolverStyle(ResolverStyle.STRICT)

  // Longueur attendue d'un timestamp yyyyMMddHHmmss
  private val ExpectedLength = 14

  /** Période de la journée : Morning [6h-12h[, Afternoon [12h-18h[, Evening [18h-22h[, Night le reste. */
  def dayPeriod(hour: Int): String =
    if (hour >= 6 && hour < 12) "Morning"
    else if (hour >= 12 && hour < 18) "Afternoon"
    else if (hour >= 18 && hour < 22) "Evening"
    else "Night"

  /**
   * Fonction pure (testable sans Spark). Renvoie None pour une chaîne nulle, vide ou mal formée.
   * workStart et workEnd : bornes incluses des heures de bureau (9 et 17 par défaut).
   */
  def compute(ts: String, workStart: Int = 9, workEnd: Int = 17): Option[TimeFeaturesResult] = {
    if (ts == null) None                                            // valeur manquante
    else {
      val s = ts.trim                                               // espaces autour ignorés
      // exactement 14 caractères, tous des chiffres : yyyyMMddHHmmss
      if (s.length != ExpectedLength || !s.forall(_.isDigit)) None
      // Try attrape l'exception d'une date impossible : on obtient None au lieu d'un échec du job
      else Try(LocalDateTime.parse(s, Formatter)).toOption.map { dt =>
        val h = dt.getHour
        val dow = dt.getDayOfWeek
        TimeFeaturesResult(
          hour = h,
          day_of_week = dow.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          month = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
          is_weekend = if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) 1 else 0,
          day_period = dayPeriod(h),
          is_working_hours = if (h >= workStart && h <= workEnd) 1 else 0
        )
      }
    }
  }

  /** UDF Spark : une entrée invalide donne un struct null, jamais une exception. */
  def extractTimeFeaturesUdf(workStart: Int, workEnd: Int): UserDefinedFunction = {
    // Échec immédiat si la configuration est incohérente, au lieu de produire is_working_hours = 0 partout
    require(workStart <= workEnd, s"workStart ($workStart) doit etre <= workEnd ($workEnd)")
    udf((ts: String) => compute(ts, workStart, workEnd).orNull)
  }
}