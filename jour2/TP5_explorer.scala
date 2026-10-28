// =====================================================================================
//  TP 5 – Explorer avant de prédire (35 min)
// =====================================================================================
//  Objectif  : trouver 3 faits dans les données qui guideront nos modèles de l'après-midi.
//  Règle d'or : Spark ne dessine pas. On AGRÈGE (groupBy) ou on ÉCHANTILLONNE (sample) dans Spark,
//  puis on envoie un petit résultat au navigateur : scripts/Viz.scala écrit des graphiques Plotly en HTML.
//  Réussi si : 4 graphiques dans out/tp5/ et 3 observations écrites.
//
//  Avant de commencer, dans spark-shell :   :load scripts/Viz.scala
//    Viz.bar(df, "x", "y", "Titre", "out/tp5/1.html")        Viz.scatter(df, "x", "y", "Titre", "fichier.html")
//    Viz.heatmap(df, "x", "y", "z", "Titre", "fichier.html", etiquettesY = Seq(...))
// =====================================================================================
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
val trips = spark.read.parquet("data/trips.parquet")
  .withColumn("heure", hour(col("tpep_pickup_datetime")))
  .withColumn("jour_semaine", dayofweek(col("tpep_pickup_datetime")))    // 1 = dimanche, 2 = lundi … 7 = samedi
  .join(zones.select(col("LocationID").as("PULocationID"), col("Borough")), "PULocationID")
trips.cache().count()

// ----- 1. Distribution des montants payés (total_amount) : histogramme -----------------
// Indice : une colonne tranche = floor(total_amount / 5) * 5, puis groupBy("tranche").count(), puis Viz.bar
val tranches: DataFrame = ???
// Viz.bar(tranches, "tranche", "count", "Distribution des montants", "out/tp5/1_histogramme.html")

// ----- 2. Tarif du compteur (fare_amount, sans pourboire ni taxes) selon la distance : nuage de points sur un ÉCHANTILLON
// Indice : trips.sample(0.03, seed = 1), puis Viz.scatter. Limiter les axes avec
//   layout = ", xaxis:{range:[0,30]}, yaxis:{range:[0,120]}"   C'est fare_amount que nous prédirons au TP 6.
val ech: DataFrame = ???
???

// ----- 3. Courses par jour de semaine et par heure : heatmap ---------------------------
// Indice : groupBy("jour_semaine", "heure").count(), puis Viz.heatmap(jh, "heure", "jour_semaine", "count", ...)
// Piège de lecture : janvier 2024 compte 5 lundis, mardis et mercredis, mais 4 des autres jours.
// Comparez les HEURES entre elles, pas les jours.
???

// ----- 4. Montant moyen par arrondissement de départ : barres -------------------------
???

// ----- 5. Vos 3 observations utiles pour PRÉDIRE LE MONTANT d'une course --------------
// Regardez bien le nuage de points : que signifie la ligne horizontale ?
// 1. ...
// 2. ...
// 3. ...

// ----- Bonus – ouvrez out/tp5/2_nuage.html : le graphique est interactif (zoom, survol). ---
// Ajoutez une 3e dimension : la couleur selon l'arrondissement (modifiez Viz.scatter, ou écrivez votre propre trace).
