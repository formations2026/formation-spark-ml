// =====================================================================================
//  TP 7 – Ce client laissera-t-il un pourboire ? (50 min)
// =====================================================================================
//  Objectif  : classer les courses payées PAR CARTE (les pourboires en espèces ne sont pas enregistrés) :
//  pourboire ou pas. Comparer Random Forest et SVM linéaire, tracer la courbe ROC.
//  Réussi si : AUC > 0,65 et courbe ROC tracée.
//  Avant de commencer :   :load scripts/Viz.scala
// =====================================================================================
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.{RandomForestClassifier, LinearSVC}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.functions.vector_to_array
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")

// ----- 1. Les données -------------------------------------------------------------------
//  - lire data/trips.parquet ; heure = hour(col("tpep_pickup_datetime"))
//  - joindre les zones pour obtenir Borough : zones.select(col("LocationID").as("PULocationID"), col("Borough"))
//  - garder trip_distance > 0 ET payment_type = 1 (carte)
//  - label = 1.0 si tip_amount > 0, sinon 0.0 : (col("tip_amount") > 0).cast("double")
//  - features : trip_distance, heure, passenger_count, manhattan (Borough === "Manhattan"), nuit (heure >= 20 || heure < 5)
// Quel est le taux de pourboire ? Les classes sont-elles équilibrées ?
val df: DataFrame = ???
println(df.agg(avg("label")).first())
val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 7)

// ----- 2. Pipeline : VectorAssembler → StandardScaler → RandomForestClassifier(numTrees 50, maxDepth 6), puis LinearSVC
val assembleur: VectorAssembler = ???
val scaler:     StandardScaler  = ???
val rf:  PipelineModel = ???
val svm: PipelineModel = ???

// ----- 3. AUC des deux modèles ----------------------------------------------------------
// Indice : new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
// « ERROR OWLQN: Failure! Resetting history » pendant LinearSVC : message interne de l'optimiseur, sans conséquence.
???

// ----- 4. Matrice de confusion (Random Forest, seuil 0,5) -------------------------------
// Indice : pred.groupBy("label").pivot("prediction").count(). Calculez précision et rappel à la main.
???

// ----- 5. Courbe ROC ----------------------------------------------------------------------
// Probabilité de la classe 1 : vector_to_array(col("probability"))(1).
// Les probabilités sont concentrées entre 0,3 et 0,7 : prenez comme seuils les QUANTILES des scores
// (scores.stat.approxQuantile("p", (0 to 50).map(_ / 50.0).toArray, 0.001)) plutôt que 20 seuils réguliers.
// Pour chaque seuil : taux de vrais positifs et taux de faux positifs → un DataFrame (fpr, tpr) → Viz.scatter(..., mode = "lines+markers")
???

// ----- 6. Sauvegarder le pipeline RANDOM FOREST dans out/modele_pourboire ----------------
// rf.write.overwrite().save("out/modele_pourboire")
// Pourquoi le Random Forest et pas LinearSVC, alors que leurs AUC sont presque égales ?
// Le TP 11 a besoin d'une PROBABILITÉ (colonne probability), que LinearSVC ne fournit pas.
???

// ----- Bonus – CrossValidator + ParamGridBuilder sur numTrees et maxDepth (3 plis). C'est plus long ! -----
// import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
???

// ----- Bonus 2 – Les vraies données : python scripts/download_official.py, puis relancez. Les résultats sont-ils aussi nets ?
