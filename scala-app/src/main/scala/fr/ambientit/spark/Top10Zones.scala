package fr.ambientit.spark

import org.apache.spark.sql.SparkSession

/** TP B2 – Le top 10 des zones, en application autonome.
  *   spark-submit --class fr.ambientit.spark.Top10Zones target/scala-2.13/formation-spark-app_2.13-0.1.0.jar ../data
  * Le master n'est PAS dans le code : il vient de spark-submit (local[*] par défaut, spark://…, yarn, k8s://…).
  */
object Top10Zones {
  def main(args: Array[String]): Unit = {
    val base = args.headOption.getOrElse("data")
    val spark = SparkSession.builder().appName("Top10Zones").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val trips = spark.read.parquet(s"$base/trips.parquet")
    val zones = Transformations.lireZones(spark, base)
    val top10 = Transformations.avecNomZone(Transformations.caParZone(trips), zones)
      .orderBy(org.apache.spark.sql.functions.desc("ca"))      // un join ne conserve pas l'ordre : retrier après
      .select("Zone", "Borough", "ca", "courses").limit(10)

    println(s"Top 10 des zones de départ (${spark.sparkContext.master}) :")
    top10.show(false)
    spark.stop()
  }
}
