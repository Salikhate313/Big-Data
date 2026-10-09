package com.ecommerce

import com.ecommerce.analytics.TimeFeatures
import org.scalatest.funsuite.AnyFunSuite

/** Tests de la fonction pure derrière l'UDF extractTimeFeatures (Membre B) ndeye wore thiandoum. */
class TimeFeaturesSpec extends AnyFunSuite {

  test("nuit de semaine : 2024-07-01 02:18:22 (lundi)") {
    val f = TimeFeatures.compute("20240701021822").get
    assert(f.hour == 2)
    assert(f.day_of_week == "Monday")
    assert(f.month == "July")
    assert(f.is_weekend == 0)
    assert(f.day_period == "Night")
    assert(f.is_working_hours == 0)
  }

  test("samedi matin à 9h : week-end et heures de bureau") {
    val f = TimeFeatures.compute("20240706093000").get
    assert(f.is_weekend == 1)
    assert(f.day_period == "Morning")
    assert(f.is_working_hours == 1)
  }

  test("bornes des périodes de la journée") {
    assert(TimeFeatures.compute("20240701055959").get.day_period == "Night")
    assert(TimeFeatures.compute("20240701060000").get.day_period == "Morning")
    assert(TimeFeatures.compute("20240701120000").get.day_period == "Afternoon")
    assert(TimeFeatures.compute("20240701180000").get.day_period == "Evening")
    assert(TimeFeatures.compute("20240701220000").get.day_period == "Night")
  }

  test("entrées nulles, vides ou mal formées : None, jamais d'exception") {
    assert(TimeFeatures.compute(null).isEmpty)
    assert(TimeFeatures.compute("").isEmpty)
    assert(TimeFeatures.compute("abc").isEmpty)
    assert(TimeFeatures.compute("2024070102182").isEmpty)       // 13 caractères
    assert(TimeFeatures.compute("20241301000000").isEmpty)      // mois 13
    assert(TimeFeatures.compute("20240230101010").isEmpty)      // 30 février
  }
}
