// TP 7 – Ce client laissera-t-il un pourboire ? (corrigé Scala)
// Dans spark-shell, à la racine du dépôt :   :load jour2/corriges/tp7_classification.scala
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.{RandomForestClassifier, LinearSVC}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.functions.vector_to_array
// Avant ce script :   :load scripts/Viz.scala   (ou spark-shell -i scripts/Viz.scala -i <ce fichier>)

// %% 1. Les données : paiements par carte, label = pourboire > 0, 5 features
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
val df = spark.read.parquet("data/trips.parquet").filter("trip_distance > 0 AND payment_type = 1")
  .withColumn("heure", hour(col("tpep_pickup_datetime")))
  .join(zones.select(col("LocationID").as("PULocationID"), col("Borough")), "PULocationID")
  .withColumn("label", (col("tip_amount") > 0).cast("double"))
  .withColumn("manhattan", (col("Borough") === "Manhattan").cast("double"))
  .withColumn("nuit", (col("heure") >= 20 || col("heure") < 5).cast("double"))
println(f"Taux de pourboire : ${df.agg(avg("label")).first().getDouble(0) * 100}%.1f %% (classes équilibrées)")
val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 7)

// %% 2. Pipelines : VectorAssembler → StandardScaler → Random Forest, puis LinearSVC
val assembleur = new VectorAssembler()
  .setInputCols(Array("trip_distance", "heure", "passenger_count", "manhattan", "nuit")).setOutputCol("features_brutes")
val scaler = new StandardScaler().setInputCol("features_brutes").setOutputCol("features")
val rf  = new Pipeline().setStages(Array(assembleur, scaler, new RandomForestClassifier().setNumTrees(50).setMaxDepth(6).setSeed(1))).fit(train)
val svm = new Pipeline().setStages(Array(assembleur, scaler, new LinearSVC().setMaxIter(50))).fit(train)
// (« ERROR OWLQN: Failure! Resetting history » pendant LinearSVC est un message interne de l'optimiseur, sans conséquence.)

// %% 3. AUC des deux modèles
val auc = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
val pred = rf.transform(test)
println(f"AUC Random Forest : ${auc.evaluate(pred)}%.3f   AUC LinearSVC : ${auc.evaluate(svm.transform(test))}%.3f")

// %% 4. Matrice de confusion (Random Forest, seuil 0,5), précision et rappel
pred.groupBy("label").pivot("prediction").count().orderBy("label").show()
val vp = pred.filter("label = 1 AND prediction = 1").count().toDouble
val fp = pred.filter("label = 0 AND prediction = 1").count()
val fn = pred.filter("label = 1 AND prediction = 0").count()
println(f"Précision = ${vp / (vp + fp)}%.3f   Rappel = ${vp / (vp + fn)}%.3f")

// %% 5. Courbe ROC : seuils = quantiles des scores (les probabilités sont concentrées entre 0,3 et 0,7)
val scores = pred.select(col("label"), vector_to_array(col("probability"))(1).as("p"))
val seuils = scores.stat.approxQuantile("p", (0 to 50).map(_ / 50.0).toArray, 0.001) :+ 1.01
val positifs = scores.filter("label = 1").count().toDouble
val negatifs = scores.filter("label = 0").count().toDouble
val roc = seuils.map { s =>
  val tpr = scores.filter(col("p") >= s && col("label") === 1).count() / positifs
  val fpr = scores.filter(col("p") >= s && col("label") === 0).count() / negatifs
  (fpr, tpr)
}
val rocDf = spark.createDataFrame(roc.toSeq).toDF("fpr", "tpr")
Viz.scatter(rocDf, "fpr", "tpr", f"Courbe ROC – pourboire (AUC = ${auc.evaluate(pred)}%.2f)", "out/tp7/roc.html", mode = "lines+markers")

// %% 6. Sauvegarder le Random Forest : le TP 11 a besoin d'une PROBABILITÉ, que LinearSVC ne fournit pas
rf.write.overwrite().save("out/modele_pourboire")
println("Modèle sauvegardé dans out/modele_pourboire")

// %% Bonus – CrossValidator + ParamGridBuilder (3 plis × 4 combinaisons = 12 entraînements : c'est plus long)
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
val rfc = new RandomForestClassifier().setSeed(1)
val grille = new ParamGridBuilder().addGrid(rfc.numTrees, Array(20, 50)).addGrid(rfc.maxDepth, Array(5, 8)).build()
val cv = new CrossValidator().setEstimator(new Pipeline().setStages(Array(assembleur, scaler, rfc)))
  .setEstimatorParamMaps(grille).setEvaluator(auc).setNumFolds(3).setParallelism(2)
val meilleur = cv.fit(train)
val best = meilleur.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages.last.asInstanceOf[org.apache.spark.ml.classification.RandomForestClassificationModel]
println(f"Meilleur : numTrees=${best.getNumTrees}, maxDepth=${best.getMaxDepth} — AUC test = ${auc.evaluate(meilleur.transform(test))}%.3f")
