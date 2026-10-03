package com.ecommerce.analytics

import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Indicateurs métier (Membre C). Toutes les méthodes prennent le DataFrame enrichi (Q3.2 + Q3.3 [+ Q3.4]). */
class Analytics(spark: SparkSession, cfg: AppConfig) {

  private def bc(df: DataFrame): DataFrame = if (cfg.enableBroadcast) broadcast(df) else df

  /** Q4.1 : KPI par marchand (les marchands sans transaction valide n'apparaissent pas). */
  def kpiMarchands(enriched: DataFrame): DataFrame = {
    val base = enriched.filter(col("merchant_name").isNotNull)
    // Équivalent d'un pivot sur age_group, avec 0 si aucune vente
    def caByAge(group: String) = round(sum(when(col("age_group") === group, col("amount")).otherwise(lit(0.0))), 2)

    val agg = base.groupBy("merchant_id", "merchant_name", "merchant_category", "region").agg(
      round(sum("amount"), 2).as("chiffre_affaires"),
      count(lit(1)).as("nb_transactions"),
      countDistinct("user_id").as("nb_clients_uniques"),
      round(avg("amount"), 2).as("montant_moyen"),
      round(sum(col("amount") * col("commission_rate")), 2).as("commission_totale"),
      caByAge(AgeGroups.Jeune).as("ca_jeune"),
      caByAge(AgeGroups.Adulte).as("ca_adulte"),
      caByAge(AgeGroups.AgeMoyen).as("ca_age_moyen"),
      caByAge(AgeGroups.Senior).as("ca_senior"),
      round(avg(col("is_suspicious")) * 100, 2).as("taux_suspectes_pct") // bonus Q3.4
    )

    val wCategory = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val wRegion = Window.partitionBy("region").orderBy(col("chiffre_affaires").desc)

    agg.withColumn("rang_ca_categorie", rank().over(wCategory))
      .withColumn("rang_ca_region", rank().over(wRegion))
      .select("merchant_id", "merchant_name", "merchant_category", "region", "chiffre_affaires", "nb_transactions",
        "nb_clients_uniques", "montant_moyen", "rang_ca_categorie", "rang_ca_region", "commission_totale",
        "ca_jeune", "ca_adulte", "ca_age_moyen", "ca_senior", "taux_suspectes_pct")
      .orderBy(col("chiffre_affaires").desc)
  }

  /** Q4.2 : matrice de rétention par cohorte (+ CA par cohorte et période : bonus). */
  def cohortRetention(enriched: DataFrame): DataFrame = {
    val tx = enriched.filter(col("transaction_date").isNotNull && col("user_id").isNotNull)
      .select(col("user_id"), col("amount"), col("transaction_date"),
        (year(col("transaction_date")) * 12 + month(col("transaction_date"))).as("month_idx"))

    val cohorts = tx.groupBy("user_id").agg(
      min("month_idx").as("cohort_idx"),
      date_format(min("transaction_date"), "yyyy-MM").as("cohort_month"))

    val cohortSize = cohorts.groupBy("cohort_month").agg(count(lit(1)).as("taille_cohorte"))

    val activity = tx.join(bc(cohorts), "user_id")
      .withColumn("period_index", (col("month_idx") - col("cohort_idx")).cast("int"))
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("nb_utilisateurs_actifs"), round(sum("amount"), 2).as("chiffre_affaires"))

    activity.join(bc(cohortSize), "cohort_month")
      .withColumn("taux_retention", round(col("nb_utilisateurs_actifs") / col("taille_cohorte") * 100, 2))
      .withColumn("revenu_moyen_par_utilisateur", round(col("chiffre_affaires") / col("nb_utilisateurs_actifs"), 2))
      .select("cohort_month", "period_index", "taille_cohorte", "nb_utilisateurs_actifs", "taux_retention",
        "chiffre_affaires", "revenu_moyen_par_utilisateur")
      .orderBy("cohort_month", "period_index")
  }

  /** Q4.2 : cohorte au meilleur taux de rétention à 3 mois (départage : plus grande cohorte, puis mois). */
  def bestCohort3m(retention: DataFrame): DataFrame =
    retention.filter(col("period_index") === 3)
      .orderBy(col("taux_retention").desc, col("taille_cohorte").desc, col("cohort_month"))
      .limit(1)
      .select(col("cohort_month"), col("taille_cohorte"),
        col("nb_utilisateurs_actifs").as("nb_utilisateurs_actifs_m3"),
        col("taux_retention").as("taux_retention_m3"))

  /** Q4.3 (bonus) : segmentation RFM. Règles d'affectation documentées dans le README. */
  def rfm(enriched: DataFrame): DataFrame = {
    val tx = enriched.filter(col("user_id").isNotNull && col("transaction_date").isNotNull)
    val perUser = tx.groupBy("user_id").agg(
      max("transaction_date").as("last_tx"),
      count(lit(1)).as("frequence"),
      round(sum("amount"), 2).as("montant"))
    val maxDate = perUser.agg(max("last_tx").as("max_date"))
    val customerSegment = enriched.select("user_id", "customer_segment").dropDuplicates("user_id")

    val base = perUser.crossJoin(maxDate)
      .withColumn("recence_jours", datediff(col("max_date"), col("last_tx")))
      .join(customerSegment, Seq("user_id"), "left")
      .withColumn("customer_segment", coalesce(col("customer_segment"), lit("Inconnu")))

    // Récence : la plus faible obtient 5 (tri décroissant) ; fréquence et montant : les plus forts obtiennent 5
    val scored = base
      .withColumn("score_r", ntile(5).over(Window.orderBy(col("recence_jours").desc, col("user_id"))))
      .withColumn("score_f", ntile(5).over(Window.orderBy(col("frequence").asc, col("user_id"))))
      .withColumn("score_m", ntile(5).over(Window.orderBy(col("montant").asc, col("user_id"))))
      .withColumn("score_rfm", concat(col("score_r").cast("string"), col("score_f").cast("string"), col("score_m").cast("string")))

    val r = col("score_r"); val f = col("score_f"); val m = col("score_m")
    scored.withColumn("segment_rfm",
      when(r <= 2 && f <= 2, "Perdus")
        .when(r <= 2, "À risque")
        .when(f <= 2, "Nouveaux")
        .when(r >= 4 && f >= 4 && m >= 4, "Champions")
        .otherwise("Clients fidèles"))
      .select("user_id", "customer_segment", "recence_jours", "frequence", "montant",
        "score_r", "score_f", "score_m", "score_rfm", "segment_rfm")
  }

  /** Q4.3 (bonus) : tableau croisé segment RFM x customer_segment. */
  def rfmByCustomerSegment(rfm: DataFrame): DataFrame = {
    val pivoted = rfm.groupBy("segment_rfm").pivot("customer_segment").count().na.fill(0L)
    val segmentCols = pivoted.columns.filter(_ != "segment_rfm")
    pivoted.withColumn("total", segmentCols.map(c => col(s"`$c`")).reduce(_ + _))
      .orderBy(col("total").desc)
  }

  /** Q4.4 (bonus) : top 10 des produits par chiffre d'affaires. */
  def top10Products(enriched: DataFrame): DataFrame = {
    val agg = enriched.filter(col("product_price").isNotNull).groupBy("product_id").agg(
      min("product_name").as("product_name"),
      min("category").as("category"),
      round(sum("amount"), 2).as("chiffre_affaires"),
      count(lit(1)).as("nb_transactions"),
      min("rating").as("rating"),
      min("stock").as("stock"))
    agg.orderBy(col("chiffre_affaires").desc).limit(10)
      .withColumn("rang", row_number().over(Window.orderBy(col("chiffre_affaires").desc)))
      .select("rang", "product_id", "product_name", "category", "chiffre_affaires", "nb_transactions", "rating", "stock")
  }

  /** Q4.4 (bonus) : CA et transactions par région x catégorie, avec part de la catégorie dans sa région. */
  def caByCategoryRegion(enriched: DataFrame): DataFrame = {
    val agg = enriched.filter(col("region").isNotNull).groupBy("region", "category").agg(
      round(sum("amount"), 2).as("chiffre_affaires"), count(lit(1)).as("nb_transactions"))
    agg.withColumn("part_ca_region_pct",
      round(col("chiffre_affaires") / sum("chiffre_affaires").over(Window.partitionBy("region")) * 100, 2))
      .orderBy(col("region"), col("chiffre_affaires").desc)
  }

  /** Q4.4 (bonus) : CA par méthode de paiement x période de la journée (valeurs absentes = UNKNOWN). */
  def caByPaymentAndPeriod(enriched: DataFrame): DataFrame = {
    val agg = enriched
      .withColumn("payment_method", coalesce(col("payment_method"), lit("UNKNOWN")))
      .withColumn("day_period", coalesce(col("day_period"), lit("UNKNOWN")))
      .groupBy("payment_method", "day_period").agg(
        round(sum("amount"), 2).as("chiffre_affaires"), count(lit(1)).as("nb_transactions"))
    agg.withColumn("part_ca_pct",
      round(col("chiffre_affaires") / sum("chiffre_affaires").over(Window.partitionBy(lit(1))) * 100, 2))
      .orderBy(col("payment_method"), col("day_period"))
  }
}
