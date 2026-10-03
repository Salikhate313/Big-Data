package com.ecommerce

import org.apache.spark.sql.SparkSession

/** SparkSession locale partagée par les tests. */
object SparkTestSession {
  lazy val spark: SparkSession = {
    com.ecommerce.utils.WindowsSupport.init()
    val s = SparkSession.builder().master("local[2]").appName("tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }
}
