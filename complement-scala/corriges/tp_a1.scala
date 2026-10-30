// Complément Scala – TP A1 : Premiers pas dans le REPL (corrigé)
// 1. 1 + 2 → Int = 3 ; "7".toInt → Int = 7 ; 7.toDouble → Double = 7.0 ; "7" + 1 → String = "71" (concaténation !)
//    List(1, 2.5) → List[Double] = List(1.0, 2.5) : Scala unifie vers le type commun
// 2.
val n = 42
// n = 43                 → error: reassignment to val
// var c = 0; c = "un"    → error: type mismatch; found: String("un") required: Int
// 3.
def tarif(distance: Double, base: Double = 3.0, parMile: Double = 1.7): Double = base + distance * parMile
println(tarif(10)); println(tarif(10, parMile = 2.0))
// 4.
val d = 17.2
println(f"Course de $d%.1f miles : ${tarif(d)}%.2f $$")
// 5.
val longue1 = (dist: Double) => dist > 10
val longue2: Double => Boolean = dist => dist > 10
val longue3: Double => Boolean = _ > 10
val distances = List(2.1, 17.2, 0.8, 12.0)
println(distances.filter(longue1)); println(distances.map(longue3))
// 6.
val montants = List(12.5, 70.0, 8.0, 25.0)
val totalTTC = montants.filter(_ > 10).map(_ * 1.2).reduce(_ + _)
println(totalTTC)
// Bonus
println(sc.parallelize(montants).filter(_ > 10).map(_ * 1.2).reduce(_ + _))   // 129.0, et un job dans la Spark UI
