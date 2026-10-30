// =====================================================================================
//  Complément Scala – TP B1 : Le fil rouge en Dataset typé (35 min)
// =====================================================================================
//  Objectif  : le TP 4 de la formation, avec le compilateur comme filet.
//  Réussi si : mêmes chiffres qu'au TP 4 (JFK 261 823 $) et 2 erreurs de compilation expliquées.
//  ⚠ Déclarez la case class AU PROMPT de spark-shell (pas via :load) avant de coller la suite.
// =====================================================================================
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import spark.implicits._

// ----- 1. Dataset[Course] ---------------------------------------------------------------------
// case class Course(PULocationID: Int, trip_distance: Double, total_amount: Double, tip_amount: Double)
val ds: Dataset[Course] = ???     // spark.read.parquet("data/trips.parquet").select(...).as[Course]
println(ds.count())

// ----- 2. Lambdas typées -------------------------------------------------------------------------
val longues = ???                 // courses > 10 miles (filter avec une lambda sur Course)
val prixMile: Dataset[Double] = ???   // total_amount / trip_distance, pour trip_distance > 0
val paires: Dataset[(Int, Double)] = ???   // (PULocationID, total_amount)

// ----- 3. Jointure + agrégation en colonnes : CA par Borough ---------------------------------------
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
val parBorough: DataFrame = ???   // ds.join(zones, ds("PULocationID") === zones("LocationID")).groupBy("Borough").agg(...)
parBorough.show()

// ----- 4. Deux explain() -------------------------------------------------------------------------
ds.filter(c => c.trip_distance > 10).explain()
ds.filter($"trip_distance" > 10).explain()
// Où est PushedFilters ? Pourquoi ?

// ----- 5. Deux erreurs de compilation que le DataFrame n'aurait pas vues -----------------------------
// ds.map(_.montan)                       → ???
// ds.filter(c => c.trip_distance > "10") → ???

// ----- Bonus ---------------------------------------------------------------------------------------
// Option[Double] pour tip_amount dans la case class (les null deviennent None) ;
// paires.groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _) : le top 10 en API typée.
