package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

  def build(cfg: AppConfig): SparkSession = {
    val builder = SparkSession.builder()
      .appName(cfg.appName)
      .config("spark.sql.shuffle.partitions", cfg.shufflePartitions.toString)
      .config("spark.sql.session.timeZone", cfg.timeZone)
      .config("spark.sql.ansi.enabled", "false") // to_timestamp renvoie null (au lieu d'échouer) si format invalide
      .config("spark.sql.debug.maxToStringFields", "1000") // supprime l'avertissement « Truncated the string representation of a plan »

    // spark-submit --master a priorité sur application.conf
    val configured = if (sys.props.contains("spark.master")) builder else builder.master(cfg.master)

    val spark = configured.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
