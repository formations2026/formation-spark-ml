// Application Spark en Scala : le fil rouge empaqueté (TP B2 et B3 du complément Scala)
//   sbt compile          compiler
//   sbt test             tests ScalaTest avec une SparkSession locale
//   sbt package          → target/scala-2.13/formation-spark-app_2.13-0.1.0.jar
//   spark-submit --class fr.ambientit.spark.Top10Zones target/scala-2.13/formation-spark-app_2.13-0.1.0.jar ../data
name := "formation-spark-app"
version := "0.1.0"
scalaVersion := "2.13.16"            // Spark 4.x est compilé pour Scala 2.13

val sparkVersion = "4.0.1"           // alignez sur votre spark-shell --version

libraryDependencies ++= Seq(
  // "provided" : fournies par le cluster au moment du spark-submit, pas embarquées dans le JAR
  "org.apache.spark" %% "spark-sql"   % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.scalatest"    %% "scalatest"   % "3.2.19"     % Test
)

// Pour exécuter `sbt run` en local, les dépendances "provided" doivent être sur le classpath :
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
Test / fork := true                  // une JVM séparée pour les tests Spark
Test / parallelExecution := false    // une seule SparkSession à la fois
javaOptions ++= Seq("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED")
