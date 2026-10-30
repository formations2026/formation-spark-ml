package fr.ambientit.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Les transformations métier : des fonctions pures DataFrame => DataFrame, testables sans cluster. */
object Transformations {

  /** Chiffre d'affaires par zone de départ, les plus rentables en premier. */
  def caParZone(trips: DataFrame): DataFrame =
    trips.groupBy("PULocationID")
      .agg(round(sum("total_amount"), 2).as("ca"), count("*").as("courses"))
      .orderBy(desc("ca"))

  /** Ajoute le nom et l'arrondissement de la zone. */
  def avecNomZone(df: DataFrame, zones: DataFrame): DataFrame =
    df.join(zones.select(col("LocationID").as("PULocationID"), col("Zone"), col("Borough")), "PULocationID")

  /** Les features du modèle de pourboire (identiques au TP 7 / TP 11). */
  def featuresPourboire(df: DataFrame, zones: DataFrame): DataFrame =
    df.filter("trip_distance > 0 AND payment_type = 1")
      .withColumn("heure", hour(col("tpep_pickup_datetime")))
      .join(zones.select(col("LocationID").as("PULocationID"), col("Borough")), "PULocationID")
      .withColumn("manhattan", (col("Borough") === "Manhattan").cast("double"))
      .withColumn("nuit", (col("heure") >= 20 || col("heure") < 5).cast("double"))
      .withColumn("label", (col("tip_amount") > 0).cast("double"))

  def lireZones(spark: SparkSession, base: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true").csv(s"$base/taxi_zone_lookup.csv")
}
