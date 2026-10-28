// TP 5 – Explorer avant de prédire (corrigé Scala)
// Dans spark-shell, à la racine du dépôt :   :load jour2/corriges/tp5_explorer.scala   → graphiques HTML dans out/tp5/
// Règle d'or : Spark ne dessine pas. On agrège (groupBy) ou on échantillonne (sample) DANS Spark,
// puis on envoie un PETIT résultat au navigateur (Plotly.js, via scripts/Viz.scala).
import org.apache.spark.sql.functions._
// Avant ce script :   :load scripts/Viz.scala   (ou spark-shell -i scripts/Viz.scala -i <ce fichier>)

// %% 0. Les données du fil rouge, enrichies de l'heure, du jour de semaine et de l'arrondissement
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
val trips = spark.read.parquet("data/trips.parquet")
  .withColumn("heure", hour(col("tpep_pickup_datetime")))
  .withColumn("jour_semaine", dayofweek(col("tpep_pickup_datetime")))    // 1 = dimanche, 2 = lundi … 7 = samedi
  .join(zones.select(col("LocationID").as("PULocationID"), col("Borough")), "PULocationID")
trips.cache()
println(s"Courses : ${trips.count()}")

// %% 1. Distribution des montants : Spark compte par tranche de 5 $, le navigateur dessine
val tranches = trips.withColumn("tranche", (floor(col("total_amount") / 5) * 5).cast("int"))
  .groupBy("tranche").count().filter("tranche < 150").orderBy("tranche")
Viz.bar(tranches, "tranche", "count", "Distribution des montants ($, par tranche de 5)", "out/tp5/1_histogramme.html")

// %% 2. Tarif du compteur selon la distance : nuage de points sur un ÉCHANTILLON de 3 %
val ech = trips.sample(0.03, seed = 1).select("trip_distance", "fare_amount")
Viz.scatter(ech, "trip_distance", "fare_amount", "Tarif selon la distance (échantillon 3 %)", "out/tp5/2_nuage.html",
            layout = ", xaxis:{range:[0,30], title:\"Distance (miles)\"}, yaxis:{range:[0,120], title:\"Tarif ($)\"}")

// %% 3. Courses par jour de semaine et par heure : heatmap (groupBy Spark → matrice 7 × 24)
val jh = trips.groupBy("jour_semaine", "heure").count()
Viz.heatmap(jh, "heure", "jour_semaine", "count", "Courses par jour et par heure", "out/tp5/3_heatmap.html",
            etiquettesY = Seq("Dim", "Lun", "Mar", "Mer", "Jeu", "Ven", "Sam"))

// %% 4. Montant moyen par arrondissement de départ
val parBorough = trips.groupBy("Borough")
  .agg(round(avg("total_amount"), 2).as("montant_moyen"), count("*").as("courses"))
  .orderBy(desc("montant_moyen"))
parBorough.show()
Viz.bar(parBorough, "Borough", "montant_moyen", "Montant moyen par arrondissement de départ", "out/tp5/4_borough.html")

// %% 5. Trois observations utiles pour prédire le montant
println("""
Observations :
1. Le tarif est presque proportionnel à la distance … sauf un palier horizontal à 70 $ : le forfait JFK.
   → un modèle linéaire ratera ce palier ; un arbre le capturera.
2. Deux pics de trafic, 8h et 18h : l'heure est une feature utile (demande, embouteillages).
3. Les aéroports (Queens / EWR) ont les montants moyens les plus élevés, Manhattan le plus de courses :
   l'arrondissement de départ est une feature utile.
""")
