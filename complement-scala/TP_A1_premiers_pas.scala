// =====================================================================================
//  Complément Scala – TP A1 : Premiers pas dans le REPL (25 min)
// =====================================================================================
//  Objectif  : écrire, exécuter et LIRE du Scala dans spark-shell.
//  Réussi si : les 6 étapes s'exécutent et vous savez lire « res3: Int => Boolean ».
//  Lancez spark-shell à la racine du dépôt ; collez chaque étape (bloc : :paste puis Ctrl+D).
// =====================================================================================

// ----- 1. Lire les types ---------------------------------------------------------------
// Tapez : 1 + 2        "7".toInt        7.toDouble        "7" + 1        List(1, 2.5)
// Pour chacun, notez le type affiché par le REPL. Surprise sur "7" + 1 ? Et sur List(1, 2.5) ?

// ----- 2. val et var --------------------------------------------------------------------
val n = 42
// Provoquez les deux erreurs de compilation vues en cours :
//   - réassigner n
//   - mettre une String dans une var déclarée avec un Int

// ----- 3. Une fonction avec valeurs par défaut -------------------------------------------
// tarif(distance, base = 3.0, parMile = 1.7) renvoie base + distance * parMile
def tarif(distance: Double, base: Double = ???, parMile: Double = ???): Double = ???
println(tarif(10))                  // 20.0
println(tarif(10, parMile = 2.0))   // 23.0

// ----- 4. Interpolation ------------------------------------------------------------------
// Pour d = 17.2, affichez :  Course de 17.2 miles : 32.24 $      (f"…", %.1f, %.2f, $$ pour le dollar)
val d = 17.2
println(???)

// ----- 5. La lambda « course longue » sous 3 formes ----------------------------------------
// une course est longue si sa distance dépasse 10 miles
val longue1 = (dist: Double) => ???
val longue2: Double => Boolean = dist => ???
val longue3: Double => Boolean = ???
val distances = List(2.1, 17.2, 0.8, 12.0)
println(distances.filter(longue1))           // List(17.2, 12.0)
println(distances.map(longue3))              // List(false, true, false, true)

// ----- 6. filter / map / reduce ------------------------------------------------------------
// total TTC (× 1.2) des courses de plus de 10 $
val montants = List(12.5, 70.0, 8.0, 25.0)
val totalTTC = ???
println(totalTTC)                            // 129.0

// ----- Bonus – la même chaîne, distribuée -----------------------------------------------
// Remplacez montants par sc.parallelize(montants) : même résultat ? Ouvrez la Spark UI (sc.uiWebUrl) : un job !
