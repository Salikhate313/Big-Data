package com.ecommerce.analytics

import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

/** Optimisations Spark pilotées par application.conf (Membre C). */
class SparkOptimizations(cfg: AppConfig) {

  /** cache() : DataFrame réutilisés plusieurs fois. */
  def cache[T](ds: Dataset[T]): Dataset[T] = if (cfg.enableCache) ds.cache() else ds

  /** persist(MEMORY_AND_DISK_SER) : DataFrame volumineux (déborde sur disque si la mémoire manque). */
  def persistSer[T](ds: Dataset[T]): Dataset[T] =
    if (cfg.enableCache) ds.persist(StorageLevel.MEMORY_AND_DISK_SER) else ds

  /** unpersist() explicite dès que le cache n'est plus utile. */
  def unpersist(datasets: Seq[Dataset[_]]): Unit = datasets.foreach(_.unpersist())

  /** broadcast() d'une petite table lors d'une jointure avec une grande table. */
  def broadcastIf(df: DataFrame): DataFrame = if (cfg.enableBroadcast) broadcast(df) else df
}
