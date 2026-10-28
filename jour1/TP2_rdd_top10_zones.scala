// =====================================================================================
//  TP 2 – Top 10 des zones avec les RDD (30 min)
// =====================================================================================
//  Objectif  : trouver les 10 zones de départ (PULocationID) qui rapportent le plus (total_amount), avec l'API RDD.
//  Colonnes de trips.csv (index à partir de 0) : PULocationID = colonne 7, total_amount = colonne 16.
//  Livrable  : ce fichier, enregistré dans le dépôt (git checkout -b prenom, puis git add / git commit).
//  Réussi si : le top 10 affiche les noms des zones, JFK Airport en tête (≈ 261 823 $).
//
//  spark-shell à la racine du dépôt ; collez chaque étape (:paste puis Ctrl+D pour un bloc).
//  Les ??? sont typés : tout compile, et chaque étape s'exécute jusqu'au premier ??? non remplacé.
// =====================================================================================
import org.apache.spark.rdd.RDD

// ----- 1. Charger data/trips.csv en RDD de lignes (sc.textFile) et retirer l'en-tête ---
// Indice : first() renvoie la première ligne ; filter(_ != entete).
val lignes:  RDD[String] = ???
val entete:  String      = ???
val courses: RDD[String] = ???
println(courses.count())

// ----- 2. Construire un PairRDD (PULocationID, total_amount) ---------------------------
// Indice : map(_.split(",")) puis map(c => (c(7).toInt, c(16).toDouble))
val paires: RDD[(Int, Double)] = ???
paires.take(3).foreach(println)

// ----- 3. Chiffre d'affaires par zone avec reduceByKey --------------------------------
val caParZone: RDD[(Int, Double)] = ???

// ----- 4. Garder le top 10 avec takeOrdered -------------------------------------------
// Indice : takeOrdered(10)(Ordering.by[(Int, Double), Double](-_._2))
val top10: Array[(Int, Double)] = ???
top10.foreach(println)

// ----- 5. Afficher le nom des zones : join avec data/taxi_zone_lookup.csv -------------
// Le fichier met tous les champs entre guillemets :  "132","Queens","JFK Airport","Airports"
//   → split(",") puis map(_.replace("\"", ""))
// Indices :
//  - comme pour trips.csv, RETIREZ L'EN-TÊTE du fichier des zones avant de convertir ;
//  - ⚠ les clés doivent avoir LE MÊME TYPE des deux côtés : c(0).toInt. Avec des clés "132" (String) d'un côté
//    et 132 (Int) de l'autre, le join renvoie un résultat VIDE, sans aucune erreur (et sans compiler en Scala :
//    le compilateur vous le dira… c'est l'avantage du typage statique) ;
//  - construire un PairRDD (LocationID, Zone), puis sc.parallelize(top10.toSeq).join(zones) ;
//  - le join ne garde pas l'ordre : retriez avec sortBy(-_._2._1).
val zones:    RDD[(Int, String)]            = ???
val resultat: RDD[(Int, (Double, String))]  = ???
resultat.collect().foreach { case (zone, (ca, nom)) => println(f"$nom%-45s $ca%,12.2f $$") }

// ----- 6. Dans la Spark UI : combien de STAGES pour le calcul du top 10 ? Où est le SHUFFLE ? -----
// Votre réponse : ...

// ----- Bonus – groupByKey contre reduceByKey -------------------------------------------
// Refaites l'étape 3 avec groupByKey().mapValues(_.sum) et lancez un count() sur chaque version.
// Dans la Spark UI, onglet Stages, comparez la colonne « Shuffle Write » des deux stages.
// Combien de fois plus de données groupByKey envoie-t-il sur le réseau ? Pourquoi ?
// Variante 100 % Scala : un SparkListener qui affiche e.stageInfo.taskMetrics.shuffleWriteMetrics.bytesWritten
// (voir le corrigé). Ne comparez pas les temps : sur 120 000 lignes et un portable, la différence est noyée dans le bruit.
???
