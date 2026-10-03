package com.ecommerce.utils

import java.io.File

object PathUtils {

  /** Résout un chemin d'entrée : chemin tel quel s'il existe (ou s'il est distant : hdfs://, s3a://...),
    * sinon recherche sous src/main/resources/ (arborescence du projet). */
  def resolveInput(path: String): String = {
    if (path.contains("://") || new File(path).exists()) path
    else {
      val alt = s"src/main/resources/$path"
      if (new File(alt).exists()) alt else path
    }
  }

  def joinPath(base: String, parts: String*): String =
    (base.stripSuffix("/") +: parts.map(_.stripPrefix("/").stripSuffix("/"))).mkString("/")
}
