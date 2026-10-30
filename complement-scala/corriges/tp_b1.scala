// Complément Scala – TP B1 : Le fil rouge en Dataset typé (corrigé)
// Au prompt :  case class Course(PULocationID: Int, trip_distance: Double, total_amount: Double, tip_amount: Double)
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import spark.implicits._
val ds: Dataset[Course] = spark.read.parquet("data/trips.parquet")
  .select("PULocationID", "trip_distance", "total_amount", "tip_amount").as[Course]
println(ds.count())
val longues = ds.filter(c => c.trip_distance > 10)
val prixMile: Dataset[Double] = ds.filter(_.trip_distance > 0).map(c => c.total_amount / c.trip_distance)
val paires: Dataset[(Int, Double)] = ds.map(c => (c.PULocationID, c.total_amount))
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
val parBorough: DataFrame = ds.join(zones, ds("PULocationID") === zones("LocationID"))
  .groupBy("Borough").agg(round(sum("total_amount"), 2).as("ca"), count("*").as("courses")).orderBy(desc("ca"))
parBorough.show()
ds.filter(c => c.trip_distance > 10).explain()     // pas de PushedFilters : la lambda est opaque pour Catalyst
ds.filter($"trip_distance" > 10).explain()         // PushedFilters: [IsNotNull(trip_distance), GreaterThan(trip_distance,10.0)]
// ds.map(_.montan)                       → value montan is not a member of Course
// ds.filter(c => c.trip_distance > "10") → type mismatch: String, required: Double
// Bonus : top 10 en API typée
paires.groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _).map { case (z, ca) => (z, ca) }
  .orderBy(desc("_2")).show(10)
