package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

/** Paramètres typés de l'application (issus de application.conf). */
case class AppConfig(
  appName: String,
  master: String,
  shufflePartitions: Int,
  timeZone: String,
  enableCache: Boolean,
  enableBroadcast: Boolean,
  transactionsPath: String,
  usersPath: String,
  productsPath: String,
  merchantsPath: String,
  outputPath: String,
  timestampLength: Int,
  minAge: Int,
  maxAge: Int,
  minRating: Double,
  maxRating: Double,
  minCommission: Double,
  maxCommission: Double,
  workingHoursStart: Int,
  workingHoursEnd: Int,
  activeMinDistinctDays: Int,
  suspiciousAmountPct: Double,
  suspiciousMaxDelayMinutes: Double,
  suspiciousMinConditions: Int
)

/** Chargement de la configuration avec valeur par défaut pour chaque clé absente.
  * Toute clé peut être surchargée en ligne de commande :
  * -Dapp.optimization.enable-cache=false */
object ConfigLoader {

  private def str(c: Config, p: String, d: String): String     = if (c.hasPath(p)) c.getString(p) else d
  private def int(c: Config, p: String, d: Int): Int           = if (c.hasPath(p)) c.getInt(p) else d
  private def dbl(c: Config, p: String, d: Double): Double     = if (c.hasPath(p)) c.getDouble(p) else d
  private def bool(c: Config, p: String, d: Boolean): Boolean  = if (c.hasPath(p)) c.getBoolean(p) else d

  def load(): AppConfig = fromConfig(ConfigFactory.load())

  def fromConfig(c: Config): AppConfig = AppConfig(
    appName            = str(c, "app.name", "EcommerceAnalytics"),
    master             = str(c, "app.spark.master", "local[*]"),
    shufflePartitions  = int(c, "app.spark.shuffle.partitions", 8),
    timeZone           = str(c, "app.spark.timezone", "UTC"),
    enableCache        = bool(c, "app.optimization.enable-cache", true),
    enableBroadcast    = bool(c, "app.optimization.enable-broadcast", true),
    transactionsPath   = str(c, "app.data.input.transactions", "data/transactions.csv"),
    usersPath          = str(c, "app.data.input.users", "data/users.json"),
    productsPath       = str(c, "app.data.input.products", "data/products.parquet"),
    merchantsPath      = str(c, "app.data.input.merchants", "data/merchants.csv"),
    outputPath         = str(c, "app.data.output.path", "output/"),
    timestampLength    = int(c, "app.validation.transaction.timestamp-length", 14),
    minAge             = int(c, "app.validation.user.min-age", 16),
    maxAge             = int(c, "app.validation.user.max-age", 100),
    minRating          = dbl(c, "app.validation.product.min-rating", 1.0),
    maxRating          = dbl(c, "app.validation.product.max-rating", 5.0),
    minCommission      = dbl(c, "app.validation.merchant.min-commission", 0.0),
    maxCommission      = dbl(c, "app.validation.merchant.max-commission", 1.0),
    workingHoursStart  = int(c, "app.features.working-hours.start", 9),
    workingHoursEnd    = int(c, "app.features.working-hours.end", 17),
    activeMinDistinctDays = int(c, "app.features.active-user.min-distinct-days", 5),
    suspiciousAmountPct       = dbl(c, "app.suspicious.amount-threshold-pct", 300.0),
    suspiciousMaxDelayMinutes = dbl(c, "app.suspicious.max-delay-minutes", 5.0),
    suspiciousMinConditions   = int(c, "app.suspicious.min-conditions", 2)
  )
}
