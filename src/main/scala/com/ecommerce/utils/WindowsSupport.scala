package com.ecommerce.utils

import java.io.File

import scala.util.Try

/** Support Windows pour Hadoop (utilisé en interne par Spark pour écrire sur le disque local).
  *
  * Sans winutils.exe + hadoop.dll (versions 3.3.x), l'écriture CSV/Parquet échoue sous Windows avec
  * `UnsatisfiedLinkError: NativeIO$Windows.createDirectoryWithMode0`. Le dossier `hadoop/bin/` du projet
  * contient ces deux fichiers ; cet objet configure `hadoop.home.dir` et `java.library.path` pour les utiliser.
  * Sans effet sous Linux / macOS. À appeler AVANT la création de la SparkSession. */
object WindowsSupport {

  val isWindows: Boolean = System.getProperty("os.name", "").toLowerCase.contains("win")

  /** Dossiers candidats, par ordre de priorité : propriété JVM, dossier du projet, variable HADOOP_HOME. */
  private def candidates: Seq[File] =
    Seq(
      Option(System.getProperty("hadoop.home.dir")),
      Some("hadoop"),
      Option(System.getenv("HADOOP_HOME"))
    ).flatten.filter(_.nonEmpty).map(new File(_))

  private def prependLibraryPath(dir: String): Unit = {
    val current = Option(System.getProperty("java.library.path")).getOrElse("")
    if (!current.split(File.pathSeparator).contains(dir)) {
      System.setProperty("java.library.path", dir + File.pathSeparator + current)
      // Force la JVM à relire java.library.path (sans effet si l'accès est refusé : sbt le fournit déjà)
      Try {
        val f = classOf[ClassLoader].getDeclaredField("sys_paths")
        f.setAccessible(true)
        f.set(null, null)
      }
    }
  }

  def init(): Unit =
    if (isWindows) {
      candidates.find(h => new File(h, "bin/winutils.exe").isFile) match {
        case Some(home) =>
          val abs = home.getAbsoluteFile
          System.setProperty("hadoop.home.dir", abs.getPath)
          val bin = new File(abs, "bin")
          if (new File(bin, "hadoop.dll").isFile) prependLibraryPath(bin.getPath)
          println(s"[WINDOWS] hadoop.home.dir = ${abs.getPath}")
        case None =>
          println("[WINDOWS] winutils.exe introuvable (dossier hadoop/bin du projet ou HADOOP_HOME) : " +
            "l'écriture des fichiers risque d'échouer.")
      }
    }
}
