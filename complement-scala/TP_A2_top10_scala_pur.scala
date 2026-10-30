// =====================================================================================
//  Complément Scala – TP A2 : Top 10 des zones en Scala pur (35 min)
// =====================================================================================
//  Objectif  : le TP 2 de la formation Spark, sur une List : même logique, zéro cluster.
//  Réussi si : JFK Airport en tête avec 261 823,16 $.
//  Colonnes de trips.csv (index à partir de 0) : PULocationID = 7, total_amount = 16.
// =====================================================================================
import scala.io.Source

// ----- 1. Lire data/trips.csv et retirer l'en-tête ------------------------------------------
// Indice : Source.fromFile("data/trips.csv").getLines().drop(1).toList
val lignes: List[String] = ???
println(lignes.size)                                   // 120000

// ----- 2. Tuples (PULocationID: Int, total_amount: Double) ---------------------------------
val courses: List[(Int, Double)] = ???
println(courses.take(3))

// ----- 3. Chiffre d'affaires par zone : groupBy puis somme --------------------------------
// Indice : groupBy(_._1), puis .map { case (zone, cs) => (zone, cs.map(_._2).sum) }   (ou mapValues)
val caParZone: Map[Int, Double] = ???

// ----- 4. Le top 10 ---------------------------------------------------------------------------
// Indice : caParZone.toList.sortBy(-_._2).take(10)
val top10: List[(Int, Double)] = ???

// ----- 5. Le nom des zones -------------------------------------------------------------------
// data/taxi_zone_lookup.csv : "132","Queens","JFK Airport","Airports" → Map[Int, String] (retirer en-tête et guillemets)
// Attention au type des clés : Int des deux côtés (en Scala, Map[Int, String].get("132") ne compile pas !)
val zones: Map[Int, String] = ???
top10.zipWithIndex.foreach { case ((zone, ca), i) =>
  println(f"${i + 1}%2d. ${zones.getOrElse(zone, "?")}%-30s $ca%,12.2f $$")
}

// ----- Bonus 1 – case class Course et for-comprehension -------------------------------------
// case class Course(zone: Int, montant: Double) ; construire la List[Course] ; refaire le top 10 avec c.zone / c.montant
???

// ----- Bonus 2 – LE MÊME CODE, distribué ----------------------------------------------------
// Remplacez Source.fromFile(...).getLines() par sc.textFile("data/trips.csv"), retirez l'en-tête avec first() + filter,
// et groupBy + sum par reduceByKey(_ + _). Comparez avec jour1/corriges/tp2_rdd.scala.
???
