ThisBuild / scalaVersion := "2.12.18"   // Spark 3.5.x est compilé pour Scala 2.12 (et 2.13)
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "com.ecommerce"

val sparkVersion = "3.5.3"

val isWindows: Boolean = sys.props("os.name").toLowerCase.contains("win")

// Encodage de la console : sous Windows, la console utilise une page de code OEM (850 en France), pas UTF-8.
// Détection automatique via `chcp` ; forçable avec la variable d'environnement CONSOLE_ENCODING (ex. UTF-8).
val consoleEncoding: String = {
  import scala.sys.process._
  import scala.util.Try
  def valid(name: String): Boolean = Try(java.nio.charset.Charset.isSupported(name)).getOrElse(false)
  sys.env.get("CONSOLE_ENCODING").filter(valid).getOrElse {
    if (!isWindows) "UTF-8"
    else Try {
      val cp = "\\d+".r.findFirstIn(Process(Seq("cmd", "/c", "chcp")).!!).get
      if (cp == "65001") "UTF-8" else s"Cp$cp"
    }.toOption.filter(valid).getOrElse("UTF-8")
  }
}

// Windows : winutils.exe + hadoop.dll (Hadoop 3.3.x) fournis dans hadoop/bin, requis par Spark pour écrire CSV/Parquet.
val windowsHadoopOptions = Def.setting {
  if (!isWindows) Seq.empty[String]
  else {
    val home = (baseDirectory.value / "hadoop").getAbsolutePath
    Seq(s"-Dhadoop.home.dir=$home", s"-Djava.library.path=$home\\bin")
  }
}

// Options JVM nécessaires pour exécuter Spark 3.5 en local avec Java 11/17/21.
val javaModuleOptions: Seq[String] =
  if (sys.props("java.specification.version") == "1.8") Nil
  else Seq(
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false",
    "-Dfile.encoding=UTF-8"
  )

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",
    scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-feature"),

    libraryDependencies ++= Seq(
      // "provided" : Spark est fourni par le cluster / spark-submit, il ne doit pas être dans le JAR.
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "com.typesafe"      % "config"     % "1.4.3",
      "org.scalatest"    %% "scalatest"  % "3.2.18" % Test
    ),

    // `sbt run` doit voir les dépendances "provided" et tourner dans une JVM dédiée.
    Compile / run := Defaults.runTask(
      Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner
    ).evaluated,
    Compile / run / fork := true,
    Compile / run / javaOptions ++= javaModuleOptions ++ windowsHadoopOptions.value ++ Seq(
      s"-Dstdout.encoding=$consoleEncoding", s"-Dsun.stdout.encoding=$consoleEncoding",
      s"-Dstderr.encoding=$consoleEncoding", s"-Dsun.stderr.encoding=$consoleEncoding"),
    Compile / run / mainClass := Some("com.ecommerce.analytics.MainApp"),

    Test / fork := true,
    Test / javaOptions ++= javaModuleOptions ++ windowsHadoopOptions.value,

    // JAR exécutable : sbt-assembly (voir CONTRIBUTIONS.md pour la justification)
    assembly / mainClass := Some("com.ecommerce.analytics.MainApp"),
    assembly / assemblyJarName := "ecommerce-analytics.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "reference.conf"                     => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    }
  )
