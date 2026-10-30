// Complément Scala – TP A3 : Modéliser le fil rouge (corrigé)
import scala.io.Source
import scala.util.{Try, Success, Failure}

sealed trait Paiement
case class Carte(numero: String = "****") extends Paiement
case object Especes extends Paiement
case class Autre(code: Int) extends Paiement
object Paiement {
  def depuisCode(code: Int): Paiement = code match { case 1 => Carte(); case 2 => Especes; case n => Autre(n) }
}
case class Course(zone: Int, distance: Double, montant: Double, pourboire: Option[Double], paiement: Paiement)

def parse(ligne: String): Try[Course] = Try {
  val c = ligne.split(",")
  Course(c(7).toInt, c(4).toDouble, c(16).toDouble,
         Option(c(13).toDouble).filter(_ > 0),                 // 0.0 → None
         Paiement.depuisCode(c(9).toInt))
}
val lignes = Source.fromFile("data/trips.csv").getLines().drop(1).toList
val resultats = lignes.map(parse)
val courses: List[Course] = resultats.collect { case Success(c) => c }
println(s"courses : ${courses.size}, rejetées : ${resultats.count(_.isFailure)}")
println(parse("a,b,c"))                                         // Failure(java.lang.ArrayIndexOutOfBoundsException …)

trait Tarification {
  def nom: String
  def prix(distance: Double): Double
  def decrire(d: Double): String = f"$nom : ${prix(d)}%.2f $$"
}
object Compteur   extends Tarification { val nom = "Compteur";    def prix(d: Double) = 3.0 + 1.7 * d }
object ForfaitJFK extends Tarification { val nom = "Forfait JFK"; def prix(d: Double) = 70.0 }
courses.take(5).foreach(c => println(List(Compteur, ForfaitJFK).map(_.decrire(c.distance)).mkString(" | ")))

def libelle(p: Paiement): String = p match {
  case Carte(_) => "carte"
  case Especes  => "espèces"
  case Autre(n) => s"autre ($n)"
}
courses.groupBy(c => libelle(c.paiement)).foreach { case (lib, cs) =>
  val taux = cs.count(_.pourboire.isDefined).toDouble / cs.size
  println(f"$lib%-12s ${cs.size}%7d courses, pourboire dans ${taux * 100}%5.1f %% des cas")
}
// Espèces : 0 % — le pourboire en espèces n'est pas saisi : d'où le filtre payment_type = 1 du TP 7.

// Bonus
implicit class RichDouble(d: Double) { def dollars: String = f"$d%.2f $$" }
println(70.0.dollars)
def verifier(c: Course): Either[String, Course] =
  if (c.distance <= 0) Left(s"distance nulle (zone ${c.zone})") else if (c.montant < 0) Left("montant négatif") else Right(c)
val verifiees = courses.map(verifier)
println(s"incohérentes : ${verifiees.count(_.isLeft)} ; exemple : ${verifiees.collectFirst { case Left(m) => m }}")
