// TP 11 – Le modèle de pourboire en temps réel + sous le capot : Catalyst, Tungsten, AQE (corrigé Scala)
//
// Prérequis : le modèle du TP 7 (out/modele_pourboire). S'il n'existe pas, ce script l'entraîne (1 minute).
// Flux : python scripts/stream_simulator.py courses --all --reset   (ou en direct, sans --all, avec MODE=live)
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.functions.vector_to_array
val MODE = sys.env.getOrElse("MODE", "availableNow")
spark.conf.set("spark.sql.shuffle.partitions", "4")
def vider(d: String): Unit = new scala.reflect.io.Directory(new java.io.File(d)).deleteRecursively()
Seq("out/checkpoints/tp11", "out/tp11").foreach(vider)
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
  .select(col("LocationID").as("PULocationID"), col("Borough"))

// %% 1. La MÊME préparation des features en batch et en streaming : une seule fonction, aucun décalage possible
def features(df: DataFrame): DataFrame =
  df.filter("trip_distance > 0 AND payment_type = 1")
    .withColumn("heure", hour(col("tpep_pickup_datetime")))
    .join(zones, "PULocationID")
    .withColumn("manhattan", (col("Borough") === "Manhattan").cast("double"))
    .withColumn("nuit", (col("heure") >= 20 || col("heure") < 5).cast("double"))
    .withColumn("label", (col("tip_amount") > 0).cast("double"))

// %% 2. Recharger le modèle du TP 7 (ML persistence)… ou l'entraîner s'il manque
val modele = if (new java.io.File("out/modele_pourboire").isDirectory) {
  println("Modèle du TP 7 rechargé"); PipelineModel.load("out/modele_pourboire")
} else {
  val m = new Pipeline().setStages(Array(
    new VectorAssembler().setInputCols(Array("trip_distance", "heure", "passenger_count", "manhattan", "nuit")).setOutputCol("features_brutes"),
    new StandardScaler().setInputCol("features_brutes").setOutputCol("features"),
    new RandomForestClassifier().setNumTrees(50).setMaxDepth(6).setSeed(1))).fit(features(spark.read.parquet("data/trips.parquet")))
  m.write.overwrite().save("out/modele_pourboire"); println("Modèle entraîné et sauvegardé dans out/modele_pourboire"); m
}

// %% 3. Appliquer le modèle sur le FLUX : transform() fonctionne sur un DataFrame en streaming
val schema = spark.read.parquet("data/trips.parquet").schema
val flux = spark.readStream.schema(schema).option("maxFilesPerTrigger", 3).json("stream/courses")
val predictions = modele.transform(features(flux))
  .withColumn("p_pourboire", vector_to_array(col("probability"))(1))        // probabilité de la classe 1
  .select("tpep_pickup_datetime", "Borough", "label", "prediction", "p_pourboire")

// %% 4. Taux de pourboire prédit vs réel, par fenêtre de 30 minutes et par arrondissement
val suivi = predictions
  .withColumn("ts", col("tpep_pickup_datetime").cast("timestamp"))          // le watermark refuse TIMESTAMP_NTZ
  .withWatermark("ts", "10 minutes")
  .groupBy(window(col("ts"), "30 minutes"), col("Borough"))
  .agg(count("*").as("courses"),
       round(avg("p_pourboire"), 3).as("taux_predit"),                      // moyenne des probabilités, pas des 0/1
       round(avg("label"), 3).as("taux_reel"))

// %% 5. foreachBatch : pour chaque micro-batch, du code batch libre (ici : écrire en Parquet + afficher)
def traiter(batch: DataFrame, id: Long): Unit = {
  batch.write.mode("append").parquet("out/tp11/suivi")
  val n = batch.count()
  if (n > 0) println(s"micro-batch $id : $n fenêtres closes écrites")
}
val writer = suivi.writeStream.outputMode("append").foreachBatch(traiter _).option("checkpointLocation", "out/checkpoints/tp11")
val q = if (MODE == "live") writer.trigger(Trigger.ProcessingTime("5 seconds")).start()
        else writer.trigger(Trigger.AvailableNow()).start()
if (MODE == "live") q.awaitTermination(120000) else q.awaitTermination()
q.stop()
val res = spark.read.parquet("out/tp11/suivi")
res.filter("Borough = 'Manhattan'")
  .select(date_format(col("window.start"), "dd/MM HH:mm").as("fenetre"), col("courses"), col("taux_predit"), col("taux_reel"))
  .orderBy("fenetre").show(12)
println("Écart moyen |prédit - réel| : " + res.select(round(avg(abs(col("taux_predit") - col("taux_reel"))), 3)).first().get(0))

// %% 6. Catalyst : le même plan logique, quelle que soit la source. Regardez PushedFilters, ReadSchema et Batched
def requete(df: DataFrame): DataFrame = df.filter("trip_distance > 10").groupBy("PULocationID").agg(avg("total_amount"))
val csv = spark.read.option("header", "true").schema(schema).csv("data/trips.csv")
val pq  = spark.read.parquet("data/trips.parquet")
println("=== CSV ===");     requete(csv).explain("formatted")
println("=== Parquet ==="); requete(pq).explain("formatted")
// Dans les DEUX cas, Catalyst pousse le filtre (PushedFilters) et ne garde que 3 colonnes (ReadSchema).
// La différence est physique : le CSV doit lire et découper chaque ligne ; Parquet saute les colonnes inutiles,
// utilise les statistiques min/max pour sauter des blocs et lit en vectorisé (Batched: true).
def chrono[T](bloc: => T): Double = { val t = System.nanoTime; bloc; (System.nanoTime - t) / 1e9 }
for ((nom, df) <- Seq("CSV" -> csv, "Parquet" -> pq))
  println(f"$nom%-8s : ${(1 to 3).map(_ => chrono(requete(df).collect())).sum / 3}%.2f s par requête")

// %% 7. AQE (Adaptive Query Execution) : le plan est réoptimisé PENDANT l'exécution, sur le MÊME objet DataFrame
println("AQE activé : " + spark.conf.get("spark.sql.adaptive.enabled"))
val r = requete(pq)
r.collect()
r.explain()       // « isFinalPlan=true » et « AQEShuffleRead coalesced » : AQE a fusionné les petites partitions

// %% 8. Tungsten : les opérateurs préfixés *(n) sont compilés ensemble en bytecode (whole-stage codegen)
val planFinal = r.queryExecution.executedPlan.toString
println("Blocs compilés par Tungsten (whole-stage codegen) : " + """\*\((\d+)\)""".r.findAllMatchIn(planFinal).map(_.group(1)).toSet.size)
