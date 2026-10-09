package com.ecommerce.analytics

import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** Libellés des tranches d'âge (partagés avec Analytics). */
object AgeGroups {
  val Jeune = "Jeune"
  val Adulte = "Adulte"
  val AgeMoyen = "Âge Moyen"
  val Senior = "Senior"
  val All = Seq(Jeune, Adulte, AgeMoyen, Senior)
}

/**
 * Enrichissement et fenêtrage des transactions (Membre B).
 *
 * Étapes de enrichTransactionData :
 *   1. jointures LEFT avec users, products et merchants (colonnes homonymes renommées) ;
 *   2. conversion du timestamp texte en date, puis appel de l'UDF des caractéristiques temporelles ;
 *   3. tranche d'âge, rang et nombre total de transactions par utilisateur (fenêtres).
 * Puis addBehaviorFeatures ajoute le montant cumulé, l'utilisateur actif et le délai entre achats.
 */
class DataTransformation(spark: SparkSession, cfg: AppConfig) {

  // UDF créée une seule fois, avec les heures de bureau lues dans la configuration
  private val extractUdf = TimeFeatures.extractTimeFeaturesUdf(cfg.workingHoursStart, cfg.workingHoursEnd)

  // broadcast : la petite table est envoyée à tous les exécuteurs, ce qui évite un shuffle de la grande table
  private def bc(df: DataFrame): DataFrame = if (cfg.enableBroadcast) broadcast(df) else df

  /** Tranche d'âge. Le sujet laisse l'âge 25 non défini : il est rattaché à « Adulte » (25 à 44 ans). */
  def ageGroup(age: Column): Column =
    when(age.isNull, lit(null).cast("string"))   // âge inconnu : valeur nulle
      .when(age < 25, lit(AgeGroups.Jeune))      // moins de 25 ans
      .when(age < 45, lit(AgeGroups.Adulte))     // de 25 à 44 ans
      .when(age < 65, lit(AgeGroups.AgeMoyen))   // de 45 à 64 ans
      .otherwise(lit(AgeGroups.Senior))          // 65 ans et plus

  /** Q3.2 : jointures (toutes en LEFT depuis les transactions valides), UDF temporelle, fenêtres, tranche d'âge. */
  def enrichTransactionData(transactions: DataFrame, users: DataFrame, products: DataFrame, merchants: DataFrame): DataFrame = {
    // Colonnes homonymes renommées pour éviter toute ambiguïté après jointure
    val usersSel = users.select("user_id", "age", "annual_income", "city", "customer_segment")
    val productsSel = products.select(
      col("product_id"), col("name").as("product_name"), col("price").as("product_price"), col("rating"), col("stock"))
    val merchantsSel = merchants.select(
      col("merchant_id"), col("name").as("merchant_name"), col("category").as("merchant_category"),
      col("region"), col("commission_rate"))

    // LEFT : une transaction valide n'est jamais perdue, même si sa référence est orpheline
    val joined = transactions
      .join(bc(usersSel), Seq("user_id"), "left")
      .join(bc(productsSel), Seq("product_id"), "left")
      .join(bc(merchantsSel), Seq("merchant_id"), "left")

    val withTime = joined
      // texte yyyyMMddHHmmss -> vrai timestamp, utilisé pour ordonner les fenêtres
      .withColumn("transaction_date", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("time_features", extractUdf(col("timestamp")))
      .select(col("*"), col("time_features.*")) // struct éclatée en colonnes simples (compatible CSV)
      .drop("time_features")

    // Fenêtre par utilisateur, ordonnée par date puis par identifiant (ordre déterministe en cas d'égalité)
    val wUser = Window.partitionBy("user_id").orderBy(col("transaction_date"), col("transaction_id"))
    // Fenêtre par utilisateur sans ordre : elle couvre toutes ses transactions
    val wUserAll = Window.partitionBy("user_id")

    val enriched = withTime
      .withColumn("age_group", ageGroup(col("age")))
      .withColumn("transaction_rank", row_number().over(wUser))              // 1 = première transaction du client
      .withColumn("total_transactions_user", count(lit(1)).over(wUserAll))   // total des transactions du client

    addBehaviorFeatures(enriched)
  }

  /** Q3.3 : montant cumulé 7 jours, utilisateur actif, délai depuis l'achat précédent. */
  def addBehaviorFeatures(df: DataFrame): DataFrame = {
    // Fenêtre 1 : 7 x 24 h glissantes (secondes) pour le montant cumulé, transaction courante incluse
    val wSeconds = Window.partitionBy("user_id")
      .orderBy(col("transaction_date").cast("long")).rangeBetween(-7L * 86400L, 0L)
    // Fenêtre 2 : 7 jours calendaires (jour courant + 6 précédents) pour compter les jours distincts
    val wDays = Window.partitionBy("user_id").orderBy(col("day_num")).rangeBetween(-6L, 0L)
    val wUser = Window.partitionBy("user_id").orderBy(col("transaction_date"), col("transaction_id"))

    // day_num : numéro du jour depuis le 01/01/1970, nécessaire pour une fenêtre exprimée en jours
    df.withColumn("day_num", datediff(to_date(col("transaction_date")), lit("1970-01-01")))
      .withColumn("montant_cumule_7j", round(sum("amount").over(wSeconds), 2))
      // nombre de jours différents où l'utilisateur a acheté pendant les 7 derniers jours
      .withColumn("distinct_days_7j", size(array_distinct(collect_list(col("day_num")).over(wDays))))
      .withColumn("is_active_user", when(col("distinct_days_7j") >= cfg.activeMinDistinctDays, 1).otherwise(0))
      // lag : date de la transaction précédente du même utilisateur (null pour la première)
      .withColumn("jours_depuis_achat_precedent",
        datediff(col("transaction_date"), lag(col("transaction_date"), 1).over(wUser)))
      .drop("day_num", "distinct_days_7j")
  }

  /** Q3.4 (bonus) : écart au panier moyen, quatre conditions et flag is_suspicious. */
  def addSuspiciousFlags(df: DataFrame): DataFrame = {
    val wAll = Window.partitionBy("user_id")
    val wUser = Window.partitionBy("user_id").orderBy(col("transaction_date"), col("transaction_id"))
    // Panier moyen historique : moyenne de toutes les transactions de l'utilisateur (transaction courante incluse)
    val avgAmount = avg("amount").over(wAll)
    val prevSeconds = lag(col("transaction_date").cast("long"), 1).over(wUser)

    df.withColumn("panier_moyen_user", round(avgAmount, 2))
      .withColumn("ecart_panier_moyen_pct", round((col("amount") - avgAmount) / avgAmount * 100, 2))
      .withColumn("delai_precedente_minutes", round((col("transaction_date").cast("long") - prevSeconds) / 60.0, 2))
      // Condition 1 : montant supérieur de plus de 300 % au panier moyen (soit plus de 4 fois la moyenne)
      .withColumn("cond_montant", when(col("amount") > avgAmount * (1 + cfg.suspiciousAmountPct / 100.0), 1).otherwise(0))
      // Condition 2 : transaction pendant la période « Night »
      .withColumn("cond_night", when(col("day_period") === "Night", 1).otherwise(0))
      // Condition 3 : moins de 5 minutes après la transaction précédente (null pour la première : condition fausse)
      .withColumn("cond_delai", when(col("delai_precedente_minutes") < cfg.suspiciousMaxDelayMinutes, 1).otherwise(0))
      // Condition 4 : paiement en crypto-monnaie
      .withColumn("cond_crypto", when(col("payment_method") === "CRYPTO", 1).otherwise(0))
      .withColumn("nb_conditions", col("cond_montant") + col("cond_night") + col("cond_delai") + col("cond_crypto"))
      // Suspecte si au moins 2 conditions sur 4 sont remplies
      .withColumn("is_suspicious", when(col("nb_conditions") >= cfg.suspiciousMinConditions, 1).otherwise(0))
  }

  /** Colonnes de l'export transactions_enrichies (ordre du sujet). */
  def exportEnriched(df: DataFrame): DataFrame = df.select(
    "transaction_id", "user_id", "product_id", "merchant_id", "amount", "timestamp", "transaction_date",
    "location", "payment_method", "category", "age", "annual_income", "city", "customer_segment", "age_group",
    "product_name", "product_price", "rating", "stock", "merchant_name", "merchant_category", "region",
    "commission_rate", "hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hours",
    "transaction_rank", "total_transactions_user", "montant_cumule_7j", "is_active_user",
    "jours_depuis_achat_precedent", "ecart_panier_moyen_pct", "is_suspicious")

  /** Transactions suspectes uniquement, triées par montant décroissant. */
  def suspiciousTransactions(df: DataFrame): DataFrame = df.filter(col("is_suspicious") === 1).select(
      "transaction_id", "user_id", "merchant_id", "transaction_date", "amount", "panier_moyen_user",
      "ecart_panier_moyen_pct", "day_period", "delai_precedente_minutes", "payment_method",
      "cond_montant", "cond_night", "cond_delai", "cond_crypto", "nb_conditions", "is_suspicious")
    .orderBy(col("amount").desc)
}