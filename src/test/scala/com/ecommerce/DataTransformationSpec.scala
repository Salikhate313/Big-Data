package com.ecommerce

import com.ecommerce.analytics.DataTransformation
import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.ConfigLoader
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

/**
 * Jointures, fenêtres et détection de comportements (Q3.2, Q3.3, Q3.4).
 * Tests de base : Salikhate. Tests complémentaires : Ndéye Woré THIANDOUM (Membre B).
 */
class DataTransformationSpec extends AnyFunSuite {

  private val spark = SparkTestSession.spark
  import spark.implicits._
  private val transformation = new DataTransformation(spark, ConfigLoader.fromConfig(ConfigFactory.empty()))

  private def tx(id: String, user: String, amount: Double, ts: String, pay: String = "CARD") =
    Transaction(id, user, "P1", "M1", Some(amount), ts, "Paris", pay, "Books")

  private val users = Seq(
    User("U1", Some(24), Some(1.0), "Paris", "Standard", Seq("Books"), "20230101"),
    User("U2", Some(25), Some(1.0), "Paris", "Standard", Seq("Books"), "20230101")).toDS().toDF()
  private val products = Seq(Product("P1", "prod", "Books", Some(9.0), "M1", Some(4.0), Some(5))).toDS().toDF()
  private val merchants = Seq(Merchant("M1", "shop", "Books", "Bretagne", Some(0.1), "20220101")).toDS().toDF()

  private val transactions = Seq(
    tx("T1", "U1", 10.0, "20240101100000"),
    tx("T2", "U1", 20.0, "20240103100000"),
    tx("T3", "U1", 30.0, "20240120100000"),
    tx("T4", "U2", 5.0, "20240101100000"),
    tx("T5", "U9", 5.0, "20240102100000")     // utilisateur orphelin
  ).toDS().toDF()

  private lazy val enriched = transformation.enrichTransactionData(transactions, users, products, merchants)

  // Enrichit une liste de transactions de l'utilisateur U1 (usage dans les tests ci-dessous)
  private def enrich(txs: Seq[Transaction]) =
    transformation.enrichTransactionData(txs.toDS().toDF(), users, products, merchants)

  // ------------------------------ tests de base ------------------------------

  test("jointures LEFT : aucune transaction perdue, l'orphelin garde des colonnes nulles") {
    assert(enriched.count() == 5)
    val orphan = enriched.filter(col("user_id") === "U9").head()
    assert(orphan.isNullAt(orphan.fieldIndex("age")))
    assert(orphan.isNullAt(orphan.fieldIndex("age_group")))
  }

  test("tranches d'âge : 24 -> Jeune, 25 -> Adulte") {
    val groups = enriched.select("user_id", "age_group").as[(String, String)].distinct().collect().toMap
    assert(groups("U1") == "Jeune")
    assert(groups("U2") == "Adulte")
  }

  test("rang, total par utilisateur, délai depuis l'achat précédent, montant cumulé 7 jours") {
    val rows = enriched.filter(col("user_id") === "U1").orderBy("transaction_date")
      .select("transaction_id", "transaction_rank", "total_transactions_user",
        "jours_depuis_achat_precedent", "montant_cumule_7j").collect()
    assert(rows.map(_.getString(0)).toSeq == Seq("T1", "T2", "T3"))
    assert(rows.map(_.getInt(1)).toSeq == Seq(1, 2, 3))
    assert(rows.forall(_.getLong(2) == 3L))
    assert(rows(0).isNullAt(3) && rows(1).getInt(3) == 2 && rows(2).getInt(3) == 17)
    assert(rows.map(_.getDouble(4)).toSeq == Seq(10.0, 30.0, 30.0))
  }

  test("les colonnes temporelles issues de l'UDF sont des colonnes simples") {
    val cols = enriched.columns.toSet
    assert(Set("hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hours").subsetOf(cols))
    assert(!cols.contains("time_features"))
  }

  test("transaction suspecte : montant énorme + nuit + crypto") {
    val many = (1 to 6).map(i => tx(s"A$i", "U1", 10.0, f"2024010${i}100000"))
    val big = tx("BIG", "U1", 5000.0, "20240110030000", "CRYPTO")
    val e = transformation.addSuspiciousFlags(
      transformation.enrichTransactionData((many :+ big).toDS().toDF(), users, products, merchants))
    val r = e.filter(col("transaction_id") === "BIG").head()
    assert(r.getAs[Int]("cond_montant") == 1)
    assert(r.getAs[Int]("cond_night") == 1)
    assert(r.getAs[Int]("cond_crypto") == 1)
    assert(r.getAs[Int]("is_suspicious") == 1)
    assert(e.filter(col("transaction_id") === "A1").head().getAs[Int]("is_suspicious") == 0)
  }

  // ------------------------- tests complémentaires -------------------------

  test("utilisateur actif : 5 jours distincts sur 7 jours") {
    val five = (1 to 5).map(i => tx(s"D$i", "U1", 10.0, s"2024020${i}100000"))
    val e = enrich(five)
    def active(id: String) = e.filter(col("transaction_id") === id).head().getAs[Int]("is_active_user")
    assert(active("D4") == 0) // 4 jours distincts
    assert(active("D5") == 1) // 5 jours distincts
  }

  test("utilisateur actif : plusieurs achats le même jour comptent pour un seul jour") {
    val sameDay = (1 to 6).map(i => tx(s"S$i", "U1", 10.0, s"202402010${i}0000"))
    assert(enrich(sameDay).filter(col("is_active_user") === 1).count() == 0)
  }

  test("fenêtre de 7 jours calendaires : jour courant + 6 précédents") {
    def lastActive(days: Seq[Int]): Int = {
      val txs = days.map(d => tx(s"W$d", "U1", 10.0, f"202402$d%02d100000"))
      enrich(txs).filter(col("transaction_id") === s"W${days.last}").head().getAs[Int]("is_active_user")
    }
    assert(lastActive(Seq(1, 2, 3, 4, 7)) == 1) // du 1er au 7 : 5 jours distincts
    assert(lastActive(Seq(1, 2, 3, 4, 8)) == 0) // du 2 au 8 : seulement 4 jours distincts
  }

  test("condition délai : moins de 5 minutes avec l'achat précédent") {
    val txs = Seq(
      tx("Q1", "U1", 10.0, "20240201100000"),
      tx("Q2", "U1", 10.0, "20240201100300"), // 3 minutes après Q1
      tx("Q3", "U1", 10.0, "20240201110000")) // 57 minutes après Q2
    val e = transformation.addSuspiciousFlags(enrich(txs))
    def delai(id: String) = e.filter(col("transaction_id") === id).head().getAs[Int]("cond_delai")
    assert(delai("Q1") == 0) // premier achat : pas de délai
    assert(delai("Q2") == 1)
    assert(delai("Q3") == 0)
  }

  test("transactions suspectes : seulement is_suspicious = 1, triées par montant décroissant") {
    val many = (1 to 6).map(i => tx(s"A$i", "U1", 10.0, s"2024010${i}100000"))
    val big1 = tx("BIG", "U1", 5000.0, "20240110030000", "CRYPTO")
    val big2 = tx("BIG2", "U1", 3000.0, "20240111030000", "CRYPTO")
    val e = transformation.addSuspiciousFlags(enrich(many :+ big1 :+ big2))
    val ids = transformation.suspiciousTransactions(e).select("transaction_id").as[String].collect().toSeq
    assert(ids == Seq("BIG", "BIG2"))
  }

  test("export transactions_enrichies : 36 colonnes dans l'ordre du sujet") {
    val out = transformation.exportEnriched(transformation.addSuspiciousFlags(enriched))
    assert(out.columns.length == 36)
    assert(out.columns.head == "transaction_id")
    assert(out.columns.last == "is_suspicious")
  }
}