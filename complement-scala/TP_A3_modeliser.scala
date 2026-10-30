// =====================================================================================
//  Complément Scala – TP A3 : Modéliser le fil rouge (35 min)
// =====================================================================================
//  Objectif  : des types qui disent la vérité sur nos courses : case class, sealed trait, Option, Try, trait.
//  Réussi si : 120 000 courses parsées, 0 null, un match que le compilateur valide.
//  Colonnes de trips.csv : passenger_count = 3, trip_distance = 4, RatecodeID = 5, PULocationID = 7,
//                          payment_type = 9, fare_amount = 10, tip_amount = 13, total_amount = 16
// =====================================================================================
import scala.io.Source
import scala.util.{Try, Success, Failure}

// ----- 1. et 2. Le modèle ----------------------------------------------------------------------
sealed trait Paiement
// case class Carte, case object Especes, case class Autre(code: Int) :
???
object Paiement {
  def depuisCode(code: Int): Paiement = ???          // 1 → Carte, 2 → Especes, sinon Autre(code)
}
// pourboire : Option[Double] (None si 0 ou absent : en espèces, il n'est pas saisi)
case class Course(zone: Int, distance: Double, montant: Double, pourboire: Option[Double], paiement: Paiement)

// ----- 3. parse : une ligne CSV → Try[Course] -------------------------------------------------
def parse(ligne: String): Try[Course] = Try {
  val c = ligne.split(",")
  ???
}
val lignes = Source.fromFile("data/trips.csv").getLines().drop(1).toList
val resultats = lignes.map(parse)
val courses: List[Course] = ???                     // les Success seulement (collect / flatMap(_.toOption))
println(s"courses : ${courses.size}, rejetées : ${resultats.count(_.isFailure)}")
// Testez parse sur une ligne volontairement cassée : que contient le Failure ?

// ----- 4. trait Tarification + 2 objets --------------------------------------------------------
trait Tarification {
  def nom: String
  def prix(distance: Double): Double
  def decrire(d: Double): String = f"$nom : ${prix(d)}%.2f $$"
}
object Compteur   extends Tarification { ??? }      // 3.0 + 1.7 × distance
object ForfaitJFK extends Tarification { ??? }      // 70.0
courses.take(5).foreach(c => println(List(Compteur, ForfaitJFK).map(_.decrire(c.distance)).mkString(" | ")))

// ----- 5. Taux de pourboire par type de paiement : un match EXHAUSTIF ----------------------------
def libelle(p: Paiement): String = p match {
  ???                                              // Carte(…) → "carte", Especes → "espèces", Autre(n) → s"autre ($n)"
}
courses.groupBy(c => libelle(c.paiement)).foreach { case (lib, cs) =>
  val taux = cs.count(_.pourboire.isDefined).toDouble / cs.size
  println(f"$lib%-12s ${cs.size}%7d courses, pourboire dans ${taux * 100}%5.1f %% des cas")
}
// Que constatez-vous sur les espèces ? (indice : les pourboires en espèces ne sont pas enregistrés par le compteur)

// ----- Bonus -------------------------------------------------------------------------------------
// a. implicit class pour écrire 70.0.dollars → "70.00 $"
// b. def verifier(c: Course): Either[String, Course] : Left si distance <= 0 ou montant < 0 ; combien de Left ?
