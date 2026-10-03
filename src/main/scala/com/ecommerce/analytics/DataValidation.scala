package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.{AppConfig, DataFrameWriterUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, SparkSession}

/** Résultat d'une validation : lignes valides + lignes rejetées (avec rejection_reason). */
case class ValidationResult[T](valid: Dataset[T], rejected: DataFrame)

case class DatasetStats(name: String, read: Long, valid: Long, nulls: Long)
case class OrphanCounts(users: Long, products: Long, merchants: Long)

/** Règles de validation de la Partie 2 (Membre A). Les seuils viennent de application.conf. */
class DataValidation(cfg: AppConfig) {

  /** Concatène les raisons de rejet ; concat_ws ignore les null => chaîne vide si aucune règle violée. */
  private def reasons(rules: Column*): Column = concat_ws(" | ", rules: _*)

  private def split[T](ds: Dataset[T], reasonCol: Column, enc: Encoder[T]): ValidationResult[T] = {
    val flagged = ds.toDF().withColumn("rejection_reason", reasonCol)
    val valid = flagged.filter(col("rejection_reason") === "").drop("rejection_reason").as[T](enc)
    val rejected = flagged.filter(col("rejection_reason") =!= "")
    ValidationResult(valid, rejected)
  }

  def validateTransactions(ds: Dataset[Transaction]): ValidationResult[Transaction] = {
    val tsPattern = s"^[0-9]{${cfg.timestampLength}}$$"
    val r = reasons(
      when(col("amount").isNull, lit("amount_manquant")).when(col("amount") <= 0, lit("amount <= 0")),
      when(col("timestamp").isNull || !col("timestamp").rlike(tsPattern), lit("timestamp_invalide"))
    )
    split(ds, r, Encoders.product[Transaction])
  }

  def validateUsers(ds: Dataset[User]): ValidationResult[User] = {
    val r = reasons(
      when(col("age").isNull || col("age") < cfg.minAge || col("age") > cfg.maxAge, lit("age_hors_intervalle")),
      when(col("annual_income").isNull || col("annual_income") <= 0, lit("income <= 0"))
    )
    split(ds, r, Encoders.product[User])
  }

  def validateProducts(ds: Dataset[Product]): ValidationResult[Product] = {
    val r = reasons(
      when(col("price").isNull || col("price") <= 0, lit("price <= 0")),
      when(col("rating").isNull || col("rating") < cfg.minRating || col("rating") > cfg.maxRating,
        lit("rating_hors_intervalle"))
    )
    split(ds, r, Encoders.product[Product])
  }

  def validateMerchants(ds: Dataset[Merchant]): ValidationResult[Merchant] = {
    val r = reasons(
      when(col("commission_rate").isNull || col("commission_rate") < cfg.minCommission ||
        col("commission_rate") > cfg.maxCommission, lit("commission_hors_intervalle"))
    )
    split(ds, r, Encoders.product[Merchant])
  }

  /** Q2.5 (bonus) : transactions dont user_id / product_id / merchant_id n'existe pas dans le référentiel lu. */
  def countOrphans(tx: DataFrame, users: DataFrame, products: DataFrame, merchants: DataFrame): OrphanCounts = {
    def orphans(ref: DataFrame, key: String): Long =
      tx.join(ref.select(key).distinct(), Seq(key), "left_anti").count()
    OrphanCounts(orphans(users, "user_id"), orphans(products, "product_id"), orphans(merchants, "merchant_id"))
  }
}

/** Rapport de qualité des données (Q2.4 + colonnes bonus Q2.5). */
object QualityReport {

  def build(spark: SparkSession, stats: Seq[DatasetStats], orphans: OrphanCounts): DataFrame = {
    import spark.implicits._
    stats.map { s =>
      val rejected = s.read - s.valid
      val isTx = s.name == "transactions"
      (s.name, s.read, s.valid, rejected, DataFrameWriterUtils.rejectRate(s.read, rejected), s.nulls,
        if (isTx) orphans.users else 0L,
        if (isTx) orphans.products else 0L,
        if (isTx) orphans.merchants else 0L)
    }.toDF("dataset", "nb_lignes_lues", "nb_lignes_valides", "nb_lignes_rejetees", "taux_rejet",
      "nb_valeurs_nulles", "nb_user_id_orphelins", "nb_product_id_orphelins", "nb_merchant_id_orphelins")
  }
}
