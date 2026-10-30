package fr.ambientit.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Tests unitaires des transformations, avec une SparkSession locale (sbt test). */
class TransformationsSpec extends AnyFunSuite with BeforeAndAfterAll {
  lazy val spark: SparkSession = SparkSession.builder().master("local[2]").appName("tests").getOrCreate()
  import spark.implicits._

  test("caParZone additionne les montants par zone et trie par CA décroissant") {
    val trips = Seq((132, 70.0), (161, 12.5), (132, 65.0)).toDF("PULocationID", "total_amount")
    val res = Transformations.caParZone(trips).collect()
    assert(res.map(_.getInt(0)).toSeq == Seq(132, 161))
    assert(res.head.getDouble(1) == 135.0)
    assert(res.head.getLong(2) == 2)
  }

  test("featuresPourboire garde les paiements par carte et calcule le label") {
    val trips = Seq(
      (132, 1, 5.0, 2.0, java.time.LocalDateTime.of(2024, 1, 15, 22, 0)),   // carte, pourboire, nuit
      (161, 2, 3.0, 0.0, java.time.LocalDateTime.of(2024, 1, 15, 10, 0)),   // espèces : exclu
      (161, 1, 3.0, 0.0, java.time.LocalDateTime.of(2024, 1, 15, 10, 0))    // carte, pas de pourboire
    ).toDF("PULocationID", "payment_type", "trip_distance", "tip_amount", "tpep_pickup_datetime")
    val zones = Seq((132, "Queens"), (161, "Manhattan")).toDF("LocationID", "Borough")
    val res = Transformations.featuresPourboire(trips, zones).orderBy("PULocationID").collect()
    assert(res.length == 2)
    assert(res(0).getAs[Double]("label") == 1.0 && res(0).getAs[Double]("nuit") == 1.0 && res(0).getAs[Double]("manhattan") == 0.0)
    assert(res(1).getAs[Double]("label") == 0.0 && res(1).getAs[Double]("manhattan") == 1.0)
  }

  override def afterAll(): Unit = spark.stop()
}
