// Complément Scala – TP A2 : Top 10 des zones en Scala pur (corrigé)
import scala.io.Source
val lignes: List[String] = Source.fromFile("data/trips.csv").getLines().drop(1).toList
println(lignes.size)
val courses: List[(Int, Double)] = lignes.map(_.split(",")).map(c => (c(7).toInt, c(16).toDouble))
println(courses.take(3))
val caParZone: Map[Int, Double] = courses.groupBy(_._1).map { case (zone, cs) => (zone, cs.map(_._2).sum) }
val top10: List[(Int, Double)] = caParZone.toList.sortBy(-_._2).take(10)
val zones: Map[Int, String] = Source.fromFile("data/taxi_zone_lookup.csv").getLines().drop(1)
  .map(_.split(",").map(_.replace("\"", ""))).map(c => (c(0).toInt, s"${c(2)} (${c(1)})")).toMap
top10.zipWithIndex.foreach { case ((zone, ca), i) =>
  println(f"${i + 1}%2d. ${zones.getOrElse(zone, "?")}%-30s $ca%,12.2f $$")
}
// Bonus 1
case class Course(zone: Int, montant: Double)
val cs = courses.map { case (z, m) => Course(z, m) }
val top10b = (for ((zone, groupe) <- cs.groupBy(_.zone)) yield (zone, groupe.map(_.montant).sum)).toList.sortBy(-_._2).take(10)
println(top10b.head)                                            // (132,261823.16)
// Bonus 2 : le même code sur un RDD
val rdd = sc.textFile("data/trips.csv"); val entete = rdd.first()
val top10rdd = rdd.filter(_ != entete).map(_.split(",")).map(c => (c(7).toInt, c(16).toDouble))
  .reduceByKey(_ + _).takeOrdered(10)(Ordering.by[(Int, Double), Double](-_._2))
println(top10rdd.head)                                          // (132,261823.16) : identique
