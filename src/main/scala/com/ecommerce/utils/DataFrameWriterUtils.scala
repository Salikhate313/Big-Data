package com.ecommerce.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

import scala.math.BigDecimal.RoundingMode

/** Fonctions réutilisées par plusieurs modules (écriture, taux de rejet, comptage de nulls). */
object DataFrameWriterUtils {

  /** Le format CSV n'accepte ni tableaux ni structures : conversion en chaîne. */
  def prepareForCsv(df: DataFrame): DataFrame =
    df.schema.fields.foldLeft(df) { (acc, f) =>
      f.dataType match {
        case _: ArrayType                => acc.withColumn(f.name, concat_ws(",", col(f.name).cast("array<string>")))
        case _: StructType | _: MapType  => acc.withColumn(f.name, to_json(col(f.name)))
        case _                           => acc
      }
    }

  /** Écrit un résultat dans <output>/csv/<name>/ (header, coalesce(1)) et <output>/parquet/<name>/ (overwrite). */
  def writeCsvAndParquet(df: DataFrame, outputPath: String, name: String, singleCsvFile: Boolean = true): Unit = {
    val csvDir     = PathUtils.joinPath(outputPath, "csv", name)
    val parquetDir = PathUtils.joinPath(outputPath, "parquet", name)
    val csvDf = prepareForCsv(df)
    (if (singleCsvFile) csvDf.coalesce(1) else csvDf)
      .write.mode(SaveMode.Overwrite).option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(csvDir)
    df.write.mode(SaveMode.Overwrite).parquet(parquetDir)
    println(s"[ECRITURE] $name -> $csvDir et $parquetDir")
  }

  /** Écrit un unique fichier CSV <outputDir>/<fileName> (coalesce(1) puis renommage du part-*.csv). */
  def writeSingleCsv(df: DataFrame, outputDir: String, fileName: String, separator: String = ","): String = {
    val tmp = PathUtils.joinPath(outputDir, s"_tmp_$fileName")
    prepareForCsv(df).coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "true").option("sep", separator).csv(tmp)

    val tmpPath = new Path(tmp)
    val fs = tmpPath.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    val part = fs.listStatus(tmpPath).map(_.getPath)
      .find(p => p.getName.startsWith("part-") && p.getName.endsWith(".csv"))
      .getOrElse(throw new IllegalStateException(s"Aucun fichier part-*.csv trouvé dans $tmp"))
    val target = new Path(PathUtils.joinPath(outputDir, fileName))
    if (fs.exists(target)) fs.delete(target, false)
    fs.rename(part, target)
    fs.delete(tmpPath, true)
    println(s"[ECRITURE] fichier unique : $target")
    target.toString
  }

  /** Pourcentage de lignes rejetées, arrondi à deux décimales. */
  def rejectRate(read: Long, rejected: Long): Double =
    if (read == 0L) 0.0
    else BigDecimal(rejected.toDouble / read * 100).setScale(2, RoundingMode.HALF_UP).toDouble

  /** Nombre total de valeurs nulles, toutes colonnes confondues. */
  def countNulls(df: DataFrame): Long = {
    val exprs = df.columns.map(c => sum(when(col(s"`$c`").isNull, 1L).otherwise(0L)).as(c))
    val row = df.select(exprs: _*).head()
    (0 until row.length).map(i => if (row.isNullAt(i)) 0L else row.getLong(i)).sum
  }
}
