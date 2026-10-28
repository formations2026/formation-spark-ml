// =====================================================================================
//  TP 8 – Les familles de courses (45 min)
// =====================================================================================
//  Objectif  : regrouper les courses en familles SANS ÉTIQUETTE, avec K-means, à partir de la distance, du montant et de l'heure.
//  Réussi si : k justifié par la silhouette et 3 familles nommées avec des mots métier.
// =====================================================================================
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator

// ----- 1. VectorAssembler (trip_distance, total_amount, heure) puis StandardScaler(withMean, withStd) -----
// Gardez les courses trip_distance > 0.
val df:         DataFrame       = ???
val assembleur: VectorAssembler = ???
val scaler:     StandardScaler  = ???

// ----- 2. KMeans k = 2 à 6 (setSeed(1)) ; silhouette de chaque k --------------------------
// Indice : new ClusteringEvaluator().setFeaturesCol("features").setMetricName("silhouette")
for (k <- 2 to 6) {
  ???
}

// ----- 3. Choisir k, décrire chaque famille --------------------------------------------
// Indice : groupBy("prediction").agg(count, avg distance, avg montant, avg heure)
// Si deux k ont une silhouette presque égale, choisissez celui dont les familles ont le plus de SENS MÉTIER.
???

// ----- 4. Nommer les familles avec des mots métier --------------------------------------
// - Famille 0 : ...
// - Famille 1 : ...
// - Famille 2 : ...

// ----- 5. Sans StandardScaler : que change-t-il ? Pourquoi ? ----------------------------
// Indice : model.stages.last.asInstanceOf[KMeansModel].clusterCenters
// ⚠ Ne comparez pas les silhouettes avec et sans normalisation : elles sont calculées dans des espaces différents.
// Pour aller plus loin : l'heure est traitée comme un nombre, donc 23 h et 0 h sont « loin ». Comment corriger cela ?
???

// ----- Bonus – GraphFrames : les zones comme un graphe ----------------------------------
// spark-shell --packages io.graphframes:graphframes-spark4_2.13:<version>  puis  import org.graphframes.GraphFrame
// Sommets = zones (id), arêtes = courses (src = PULocationID, dst = DOLocationID). Top 5 PageRank.
// Si le package manque : degrés entrants avec groupBy("dst").count().
???
