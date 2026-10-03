package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.{AppConfig, PathUtils}
import org.apache.spark.sql.functions.{col, lit, trim, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}

import scala.util.control.NonFatal

/** Erreur d'ingestion (fichier introuvable, structure incorrecte...). */
class DataIngestionException(message: String, cause: Throwable) extends RuntimeException(message, cause)

/** Lecture centralisée des quatre sources de données (Membre A). */
class DataIngestion(spark: SparkSession, cfg: AppConfig) {

  import spark.implicits._

  // Schéma explicite pour transactions.csv (Q2.1)
  private val transactionSchema = StructType(Seq(
    StructField("transaction_id", StringType, nullable = true),
    StructField("user_id", StringType, nullable = true),
    StructField("product_id", StringType, nullable = true),
    StructField("merchant_id", StringType, nullable = true),
    StructField("amount", DoubleType, nullable = true),
    StructField("timestamp", StringType, nullable = true),   // gardé en String : yyyyMMddHHmmss
    StructField("location", StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category", StringType, nullable = true)
  ))

  // Schéma de users.json : preferred_categories est un tableau (champ imbriqué)
  private val userSchema = StructType(Seq(
    StructField("user_id", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("annual_income", DoubleType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("customer_segment", StringType, nullable = true),
    StructField("preferred_categories", ArrayType(StringType, containsNull = true), nullable = true),
    StructField("registration_date", StringType, nullable = true)
  ))

  /** Aplatit d'éventuelles structures imbriquées (colonne parent_champ). No-op si aucune struct. */
  private def flattenStructs(df: DataFrame): DataFrame =
    df.schema.fields.collect { case StructField(n, st: StructType, _, _) => (n, st) }
      .foldLeft(df) { case (d, (n, st)) =>
        st.fieldNames.foldLeft(d)((acc, f) => acc.withColumn(s"${n}_$f", col(s"$n.$f"))).drop(n)
      }

  /** Certaines cellules contiennent le texte « NA » : c'est une valeur manquante, on la convertit en null. */
  private def naToNull(df: DataFrame): DataFrame =
    df.schema.fields.filter(_.dataType == StringType).foldLeft(df) { (d, f) =>
      d.withColumn(f.name, when(trim(col(f.name)) === "NA", lit(null).cast("string")).otherwise(col(f.name)))
    }

  /** Lecture protégée : affiche l'erreur puis la propage sous forme de DataIngestionException. */
  private def safeRead[T](name: String, path: String)(reader: String => Dataset[T]): Dataset[T] = {
    val resolved = PathUtils.resolveInput(path)
    try {
      println(s"[INGESTION] Lecture de $name depuis $resolved")
      reader(resolved)
    } catch {
      case e: AnalysisException =>
        println(s"[ERREUR] $name : fichier introuvable ou structure incorrecte ($resolved) : ${e.getMessage}")
        throw new DataIngestionException(s"Lecture de $name impossible : $resolved", e)
      case NonFatal(e) =>
        println(s"[ERREUR] $name : erreur inattendue à la lecture de $resolved : ${e.getMessage}")
        throw new DataIngestionException(s"Erreur de lecture de $name : $resolved", e)
    }
  }

  def readTransactions(): Dataset[Transaction] = safeRead("transactions", cfg.transactionsPath) { p =>
    naToNull(spark.read.option("header", "true").option("mode", "PERMISSIVE")
      .schema(transactionSchema).csv(p)).as[Transaction]
  }

  def readUsers(): Dataset[User] = safeRead("users", cfg.usersPath) { p =>
    flattenStructs(spark.read.option("mode", "PERMISSIVE").schema(userSchema).json(p)).as[User]
  }

  def readProducts(): Dataset[Product] = safeRead("products", cfg.productsPath) { p =>
    spark.read.parquet(p).as[Product]
  }

  def readMerchants(): Dataset[Merchant] = safeRead("merchants", cfg.merchantsPath) { p =>
    // Schéma inféré (Q2.1) ; on force les types attendus par la case class
    naToNull(spark.read.option("header", "true").option("inferSchema", "true").csv(p))
      .withColumn("establishment_date", col("establishment_date").cast("string"))
      .withColumn("commission_rate", col("commission_rate").cast("double"))
      .as[Merchant]
  }

  /** Affiche et renvoie le nombre de lignes lues avant validation (Q2.3). */
  def logRowCount(name: String, ds: Dataset[_]): Long = {
    val n = ds.count()
    println(s"[INGESTION] $name : $n lignes lues avant validation")
    n
  }
}
