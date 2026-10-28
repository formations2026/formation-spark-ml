// =====================================================================================
//  TP 6 – Combien va coûter cette course ? (30 min)
// =====================================================================================
//  Objectif  : prédire fare_amount à partir de la distance, de l'heure, du nombre de passagers et de
//  l'arrondissement de départ. Comparer une régression linéaire et un arbre de régression.
//  Réussi si : RMSE de l'arbre < 5 $ sur le test.
// =====================================================================================
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")

// ----- 1. Les données : ajouter heure et Borough (jointure avec les zones), garder trip_distance > 0 -----
// Colonnes à garder : trip_distance, heure, passenger_count, Borough, fare_amount
val df: DataFrame = ???   // spark.read.parquet("data/trips.parquet").filter(...).withColumn("heure", ...).join(...).select(...)

// ----- 2. Train / test : 80 / 20, seed = 42 ---------------------------------------------
val Array(train, test): Array[DataFrame] = ???
println(s"${train.count()} / ${test.count()}")

// ----- 3. La préparation : StringIndexer (Borough → index, handleInvalid "keep") → OneHotEncoder → VectorAssembler
// Indice : new VectorAssembler().setInputCols(Array("trip_distance", "heure", "passenger_count", "borough_vec")).setOutputCol("features")
// Les modèles cherchent par défaut une colonne nommée features.
val preparation: Array[PipelineStage] = Array(
  ???
)

// ----- 4. Un pipeline par modèle : LinearRegression, puis DecisionTreeRegressor(maxDepth = 6) -----
// Indice : new Pipeline().setStages(preparation :+ modele).fit(train), puis .transform(test)
// Les deux modèles prennent .setLabelCol("fare_amount").
val lin:   PipelineModel = ???
val arbre: PipelineModel = ???

// ----- 5. Comparer RMSE et R² sur le test ----------------------------------------------
// Indice : new RegressionEvaluator().setLabelCol("fare_amount").setMetricName("rmse")  (puis "r2")
// Lequel est le meilleur ? Pourquoi ? (indice : le palier à 70 $ du TP 5 ; quel arrondissement contient JFK ?)
???

// ----- 6. Sauvegarder le meilleur pipeline, puis le recharger ---------------------------
// Indice : modele.write.overwrite().save("out/modele_montant"), PipelineModel.load(...)
???

// ----- Bonus 1 – GBTRegressor fait-il mieux ? Et un arbre avec LA DISTANCE SEULE ? ------
// Que vous apprend la comparaison sur l'utilité de l'heure et des passagers ?
???

// ----- Bonus 2 – Ajoutez la colonne RatecodeID (1 = tarif normal, 2 = forfait JFK) aux features. -----
// Que devient la RMSE ? Qu'en concluez-vous sur l'importance de CONNAÎTRE LE MÉTIER ?
???

// ----- Bonus 3 – « Tricher » : ajoutez total_amount aux features. ----------------------
// Quel R² ? Pourquoi ce modèle est-il inutilisable ? Et pourquoi le R² n'atteint-il pas tout à fait 1 ?
// (indice : que contient total_amount en plus du tarif ?)
???
