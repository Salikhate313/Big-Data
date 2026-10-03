package com.ecommerce.analytics

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable
import scala.util.control.NonFatal

/** Orchestration du pipeline complet (Membre C) : ingestion -> validation -> transformation -> analytique -> écriture. */
class EcommercePipeline(spark: SparkSession, cfg: AppConfig) {

  private val ingestion = new DataIngestion(spark, cfg)
  private val validation = new DataValidation(cfg)
  private val transformation = new DataTransformation(spark, cfg)
  private val analytics = new Analytics(spark, cfg)
  private val opt = new SparkOptimizations(cfg)

  val timer = new StepTimer()
  private val tracked = mutable.ListBuffer.empty[Dataset[_]]

  private def track[T](ds: Dataset[T]): Dataset[T] = { tracked += ds; ds }

  /** Affiche le nombre de lignes (ce qui matérialise aussi le cache) puis un aperçu. */
  private def display(name: String, df: DataFrame, rows: Int = 20): Unit = {
    println(s"\n=== $name (${df.count()} lignes) ===")
    df.show(rows, truncate = false)
  }

  private val stageLevel = Map("ingestion" -> 1, "transformation" -> 2, "analytics" -> 3, "all" -> 3)

  /** Exécute le pipeline jusqu'à l'étape demandée (chaque étape inclut les précédentes, dont elle dépend). */
  def run(stage: String): Seq[StepTiming] = {
    val level = stageLevel.getOrElse(stage, 3)
    val results = mutable.LinkedHashMap.empty[String, DataFrame]
    var qualityReport: Option[DataFrame] = None
    try {
      // ---------- 1. INGESTION ----------
      val (txRaw, usersRaw, productsRaw, merchantsRaw, counts) = timer.time("ingestion") {
        val tx = track(opt.cache(ingestion.readTransactions()))
        val us = track(opt.cache(ingestion.readUsers()))
        val pr = track(opt.cache(ingestion.readProducts()))
        val me = track(opt.cache(ingestion.readMerchants()))
        val c = Map(
          "transactions" -> ingestion.logRowCount("transactions", tx),
          "users" -> ingestion.logRowCount("users", us),
          "products" -> ingestion.logRowCount("products", pr),
          "merchants" -> ingestion.logRowCount("merchants", me))
        (tx, us, pr, me, c)
      }

      // ---------- 2. VALIDATION ----------
      val (vTx, vUsers, vProducts, vMerchants) = timer.time("validation") {
        val t = validation.validateTransactions(txRaw)
        val u = validation.validateUsers(usersRaw)
        val p = validation.validateProducts(productsRaw)
        val m = validation.validateMerchants(merchantsRaw)
        Seq(t.valid, u.valid, p.valid, m.valid).foreach(d => track(opt.cache(d)))
        Seq(t.rejected, u.rejected, p.rejected, m.rejected).foreach(d => track(opt.cache(d)))

        val stats = Seq(
          DatasetStats("transactions", counts("transactions"), t.valid.count(), DataFrameWriterUtils.countNulls(txRaw.toDF())),
          DatasetStats("users", counts("users"), u.valid.count(), DataFrameWriterUtils.countNulls(usersRaw.toDF())),
          DatasetStats("products", counts("products"), p.valid.count(), DataFrameWriterUtils.countNulls(productsRaw.toDF())),
          DatasetStats("merchants", counts("merchants"), m.valid.count(), DataFrameWriterUtils.countNulls(merchantsRaw.toDF())))
        stats.foreach(s => println(s"[VALIDATION] ${s.name} : ${s.valid} lignes valides sur ${s.read} lues"))

        val orphans = validation.countOrphans(txRaw.toDF(), usersRaw.toDF(), productsRaw.toDF(), merchantsRaw.toDF())
        val report = QualityReport.build(spark, stats, orphans)
        println("\n=== RAPPORT DE QUALITE DES DONNEES ===")
        report.show(truncate = false)
        qualityReport = Some(report)

        results += ("rejets_transactions" -> t.rejected, "rejets_users" -> u.rejected,
          "rejets_products" -> p.rejected, "rejets_merchants" -> m.rejected)
        (t, u, p, m)
      }

      // ---------- 3. TRANSFORMATION ----------
      val enriched: Option[DataFrame] = if (level >= 2) Some(timer.time("transformation") {
        val e0 = transformation.enrichTransactionData(
          vTx.valid.toDF(), vUsers.valid.toDF(), vProducts.valid.toDF(), vMerchants.valid.toDF())
        val e = track(opt.persistSer(transformation.addSuspiciousFlags(e0)))
        display("TRANSACTIONS ENRICHIES", transformation.exportEnriched(e), 10)

        val suspicious = track(opt.cache(transformation.suspiciousTransactions(e)))
        println(s"\n[SUSPECTES] ${suspicious.count()} transactions suspectes détectées ; 20 montants les plus élevés :")
        suspicious.select("transaction_id", "user_id", "amount", "day_period", "payment_method", "nb_conditions")
          .show(20, truncate = false)

        results += ("transactions_enrichies" -> transformation.exportEnriched(e), "transactions_suspectes" -> suspicious)
        e.toDF()
      }) else None

      // ---------- 4. ANALYTIQUE ----------
      if (level >= 3) enriched.foreach { e =>
        timer.time("analytique") {
          def result(name: String, df: DataFrame): DataFrame = {
            val cached = track(opt.cache(df)).toDF()
            display(name.toUpperCase, cached, 10)
            results += (name -> cached)
            cached
          }
          result("kpi_marchands", analytics.kpiMarchands(e))
          val retention = result("cohortes_retention", analytics.cohortRetention(e))
          result("meilleure_cohorte_3m", analytics.bestCohort3m(retention))
          val rfm = result("rfm_clients", analytics.rfm(e))
          result("rfm_x_customer_segment", analytics.rfmByCustomerSegment(rfm))
          result("top10_produits", analytics.top10Products(e))
          result("ca_categorie_region", analytics.caByCategoryRegion(e))
          result("ca_paiement_periode", analytics.caByPaymentAndPeriod(e))
        }
      }

      // ---------- 5. ECRITURE ----------
      timer.time("ecriture") {
        val dateStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        qualityReport.foreach(r => DataFrameWriterUtils.writeSingleCsv(r, cfg.outputPath, s"rapport_qualite_$dateStr.csv"))
        results.foreach { case (name, df) => DataFrameWriterUtils.writeCsvAndParquet(df, cfg.outputPath, name) }
      }
      timer.timings
    } finally {
      opt.unpersist(tracked.toList) // libération explicite du cache
      tracked.clear()
    }
  }
}

object MainApp {

  val ValidStages = Seq("all", "ingestion", "transformation", "analytics", "benchmark")

  private def printHelp(bad: String): Unit = {
    println(s"Argument inconnu : '$bad'")
    println("Usage : spark-submit --class com.ecommerce.analytics.MainApp app.jar [etape]")
    println("Etapes acceptées :")
    println("  all            pipeline complet (défaut)")
    println("  ingestion      ingestion + validation + rapport de qualité + rejets")
    println("  transformation ... + enrichissement, fenêtrage, transactions suspectes")
    println("  analytics      ... + KPI, cohortes, RFM, produits (= all)")
    println("  benchmark      pipeline sans puis avec optimisations, tableau comparatif (Q5.3)")
  }

  def main(args: Array[String]): Unit = {
    val stage = args.headOption.map(_.trim.toLowerCase).getOrElse("all")
    if (!ValidStages.contains(stage)) printHelp(stage)
    else {
      val code = run(stage)
      if (code != 0) sys.exit(code)
    }
  }

  /** Exécute une étape avec arrêt propre de la SparkSession même en cas d'échec. */
  def run(stage: String): Int = {
    var spark: SparkSession = null
    try {
      WindowsSupport.init() // winutils / hadoop.dll sous Windows (no-op ailleurs), avant toute classe Hadoop
      val cfg = ConfigLoader.load()
      spark = SparkSessionBuilder.build(cfg)
      if (stage == "benchmark") benchmark(spark, cfg)
      else {
        val pipeline = new EcommercePipeline(spark, cfg)
        pipeline.run(stage)
        printTimings(pipeline.timer.timings)
      }
      println("\n[OK] Traitement terminé.")
      0
    } catch {
      case NonFatal(e) =>
        Console.err.println(s"\n[ECHEC] ${e.getClass.getSimpleName} : ${e.getMessage}")
        1
    } finally {
      if (spark != null) spark.stop()
    }
  }

  private def printTimings(timings: Seq[StepTiming]): Unit = {
    println("\n=== DUREE DES ETAPES ===")
    timings.foreach(t => println(f"${t.step}%-16s ${t.durationMs}%8d ms"))
  }

  /** Q5.3 : exécution sans puis avec optimisations, tableau avant/après. */
  private def benchmark(spark: SparkSession, cfg: AppConfig): Unit = {
    import spark.implicits._
    println("\n########## BENCHMARK 1/2 : SANS cache ni broadcast ##########")
    val base = new EcommercePipeline(spark, cfg.copy(enableCache = false, enableBroadcast = false)).run("all")
    spark.catalog.clearCache()
    println("\n########## BENCHMARK 2/2 : AVEC cache et broadcast ##########")
    val optimized = new EcommercePipeline(spark, cfg.copy(enableCache = true, enableBroadcast = true)).run("all")

    val rows = base.zip(optimized).map { case (a, b) =>
      (a.step, a.durationMs, b.durationMs, gain(a.durationMs, b.durationMs))
    } :+ {
      val (ta, tb) = (base.map(_.durationMs).sum, optimized.map(_.durationMs).sum)
      ("TOTAL", ta, tb, gain(ta, tb))
    }
    println("\n| étape | sans optimisation (ms) | avec optimisation (ms) | gain (%) |")
    println("|---|---:|---:|---:|")
    rows.foreach { case (s, a, b, g) => println(s"| $s | $a | $b | $g |") }
    val df = rows.toDF("etape", "duree_sans_optimisation_ms", "duree_avec_optimisation_ms", "gain_pct")
    DataFrameWriterUtils.writeSingleCsv(df, cfg.outputPath, "benchmark_optimisations.csv")
  }

  private def gain(before: Long, after: Long): Double =
    if (before == 0L) 0.0 else BigDecimal((before - after).toDouble / before * 100).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
}

/** Alias du nom donné dans l'énoncé (Q6.1). */
object EcommerceAnalyticsApp {
  def main(args: Array[String]): Unit = MainApp.main(args)
}
