// =====================================================================================
//  TP 11 – Le modèle de pourboire en temps réel, puis sous le capot (55 min)
// =====================================================================================
//  Objectif  : appliquer sur le flux de courses le modèle entraîné au TP 7, puis lire et comparer des plans d'exécution.
//  Réussi si : des prédictions sont produites en continu et vous savez expliquer le plan.
//
//  AVANT DE COMMENCER :  python scripts/stream_simulator.py courses --reset      (ou --all --reset)
// =====================================================================================
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.functions.vector_to_array
spark.conf.set("spark.sql.shuffle.partitions", "4")
def vider(d: String): Unit = new scala.reflect.io.Directory(new java.io.File(d)).deleteRecursively()
def chrono[T](bloc: => T): Double = { val t = System.nanoTime; bloc; (System.nanoTime - t) / 1e9 }
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
  .select(col("LocationID").as("PULocationID"), col("Borough"))

// ----- 1. La préparation des features, dans UNE fonction réutilisable en batch et en streaming -----
// Reprenez les étapes du TP 7 : carte uniquement, heure, jointure avec zones pour Borough, manhattan, nuit, label (pourboire > 0).
def features(df: DataFrame): DataFrame =
  df.filter("trip_distance > 0 AND payment_type = 1")
    .withColumn("heure", hour(col("tpep_pickup_datetime")))
    .join(zones, "PULocationID")
    ???

// ----- 2. Recharger le modèle du TP 7 : PipelineModel.load("out/modele_pourboire") ----------
// S'il n'existe pas (ou si vous y avez sauvegardé le LinearSVC), réentraînez le Random Forest du TP 7
// et sauvegardez-le à cet endroit : le TP a besoin de la colonne probability.
val modele: PipelineModel = ???

// ----- 3. Appliquer le modèle sur le flux --------------------------------------------------
// modele.transform(features(flux)), puis p_pourboire = vector_to_array(col("probability"))(1)
val schema      = spark.read.parquet("data/trips.parquet").schema
val flux:        DataFrame = ???
val predictions: DataFrame = ???

// ----- 4. Taux de pourboire PRÉDIT (moyenne de p_pourboire) vs RÉEL (moyenne de label), par fenêtre de 30 min et par Borough
// Watermark de 10 minutes sur ts = col("tpep_pickup_datetime").cast("timestamp") : le watermark refuse le type
// TIMESTAMP_NTZ (sans fuseau) de nos données, d'où la conversion.
// Pourquoi la moyenne des probabilités et pas des prédictions 0/1 ?
val suivi: DataFrame = ???

// ----- 5. foreachBatch en outputMode("append") ----------------------------------------------
// Pour chaque micro-batch : écrire en Parquet (out/tp11/suivi) et afficher le nombre de fenêtres.
// Le premier micro-batch écrit 0 fenêtre : aucune n'est encore close par le watermark. C'est normal.
Seq("out/checkpoints/tp11", "out/tp11").foreach(vider)
def traiter(batch: DataFrame, id: Long): Unit = {
  ???
}
val q: StreamingQuery = ???   // suivi.writeStream.outputMode("append").foreachBatch(traiter _).option("checkpointLocation", ...).trigger(...).start()
q.awaitTermination(120000)
q.stop()

// ----- 6. Sous le capot : explain("formatted") sur la même requête, en CSV puis en Parquet -----
def requete(df: DataFrame): DataFrame = df.filter("trip_distance > 10").groupBy("PULocationID").agg(avg("total_amount"))
val csv = spark.read.option("header", "true").schema(schema).csv("data/trips.csv")
val pq  = spark.read.parquet("data/trips.parquet")
// Comparez PushedFilters, ReadSchema et Batched. Surprise : Catalyst pousse le filtre et élague les colonnes
// DANS LES DEUX CAS. Qu'est-ce qui diffère, alors ? Mesurez le temps de chaque version (moyenne de 3 exécutions).
???

// ----- 7. AQE : le plan final n'existe qu'APRÈS l'exécution, sur LE MÊME objet DataFrame ------
//   val r = requete(pq) ; r.collect() ; r.explain()
// Trouvez isFinalPlan=true et AQEShuffleRead coalesced. Combien de blocs *(n) (whole-stage codegen) ?
// (Avec requete(pq).explain() vous obtiendriez un nouvel objet, encore isFinalPlan=false.)
???

// ----- Bonus – Spark UI, onglet Structured Streaming : débit, durée des micro-batches, lignes d'état -----
