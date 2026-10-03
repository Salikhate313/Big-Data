package com.ecommerce

import com.ecommerce.analytics.DataValidation
import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.{ConfigLoader, DataFrameWriterUtils}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

/** Règles de validation (Membre A). */
class DataValidationSpec extends AnyFunSuite {

  private val spark = SparkTestSession.spark
  import spark.implicits._
  private val validation = new DataValidation(ConfigLoader.fromConfig(ConfigFactory.empty()))

  test("transactions : montant négatif, timestamp invalide, et cumul des raisons") {
    val ds = Seq(
      Transaction("T1", "U1", "P1", "M1", Some(10.0), "20240701021822", "Paris", "CARD", "Books"),
      Transaction("T2", "U1", "P1", "M1", Some(-5.0), "20240701021822", "Paris", "CARD", "Books"),
      Transaction("T3", "U1", "P1", "M1", Some(10.0), "202407010218", "Paris", "CARD", "Books"),
      Transaction("T4", "U1", "P1", "M1", Some(0.0), null, "Paris", "CARD", "Books")
    ).toDS()
    val r = validation.validateTransactions(ds)
    assert(r.valid.count() == 1)
    assert(r.rejected.count() == 3)
    val reasons = r.rejected.select("transaction_id", "rejection_reason").as[(String, String)].collect().toMap
    assert(reasons("T2") == "amount <= 0")
    assert(reasons("T3") == "timestamp_invalide")
    assert(reasons("T4") == "amount <= 0 | timestamp_invalide")
  }

  test("users : âge hors intervalle et revenu <= 0") {
    val ds = Seq(
      User("U1", Some(30), Some(30000.0), "Paris", "Standard", Seq("Books"), "20230101"),
      User("U2", Some(12), Some(30000.0), "Paris", "Standard", Seq("Books"), "20230101"),
      User("U3", Some(140), Some(-1.0), "Paris", "Standard", Seq("Books"), "20230101")
    ).toDS()
    val r = validation.validateUsers(ds)
    assert(r.valid.count() == 1)
    val reasons = r.rejected.select("user_id", "rejection_reason").as[(String, String)].collect().toMap
    assert(reasons("U2") == "age_hors_intervalle")
    assert(reasons("U3") == "age_hors_intervalle | income <= 0")
  }

  test("products et merchants") {
    val p = Seq(
      Product("P1", "a", "Books", Some(10.0), "M1", Some(4.0), Some(3)),
      Product("P2", "b", "Books", Some(0.0), "M1", Some(6.0), Some(3))
    ).toDS()
    val rp = validation.validateProducts(p)
    assert(rp.valid.count() == 1)
    assert(rp.rejected.select("rejection_reason").as[String].head() == "price <= 0 | rating_hors_intervalle")

    val m = Seq(
      Merchant("M1", "x", "Books", "Bretagne", Some(0.05), "20220101"),
      Merchant("M2", "y", "Books", "Bretagne", Some(1.5), "20220101")
    ).toDS()
    val rm = validation.validateMerchants(m)
    assert(rm.valid.count() == 1)
    assert(rm.rejected.select("rejection_reason").as[String].head() == "commission_hors_intervalle")
  }

  test("taux de rejet arrondi à 2 décimales et comptage des nulls") {
    assert(DataFrameWriterUtils.rejectRate(3, 1) == 33.33)
    assert(DataFrameWriterUtils.rejectRate(0, 0) == 0.0)
    val df = Seq(("a", Some(1)), (null, None), ("c", None)).toDF("x", "y")
    assert(DataFrameWriterUtils.countNulls(df) == 3)
  }

  test("orphelins : références absentes des référentiels") {
    val tx = Seq(
      Transaction("T1", "U1", "P1", "M1", Some(1.0), "20240701021822", "", "", ""),
      Transaction("T2", "U9", "P1", "M9", Some(1.0), "20240701021822", "", "", "")
    ).toDS().toDF()
    val users = Seq(User("U1", Some(30), Some(1.0), "", "", Seq(), "")).toDS().toDF()
    val products = Seq(Product("P1", "", "", Some(1.0), "", Some(3.0), Some(1))).toDS().toDF()
    val merchants = Seq(Merchant("M1", "", "", "", Some(0.1), "")).toDS().toDF()
    val o = validation.countOrphans(tx, users, products, merchants)
    assert(o.users == 1 && o.products == 0 && o.merchants == 1)
  }
}
