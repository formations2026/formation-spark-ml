// =====================================================================================
//  TP 10 – Les logs en temps réel (35 min)
// =====================================================================================
//  L'application de réservation écrit ses logs dans stream/logs/. Une panne survient quelque part dans la matinée.
//  Objectif  : la détecter en temps réel.
//  Réussi si : une alerte (taux d'erreur > 10 %) apparaît sur le bon endpoint, aux bonnes minutes.
//
//  AVANT DE COMMENCER :  python scripts/stream_simulator.py logs --reset      (ou --all --reset)
// =====================================================================================
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
spark.conf.set("spark.sql.shuffle.partitions", "4")
def vider(d: String): Unit = new scala.reflect.io.Directory(new java.io.File(d)).deleteRecursively()

// ----- 1. Lire les logs en continu (readStream.text) : une colonne value par ligne ------
// Exemple de ligne :  2024-01-15T08:22:03.120 ERROR endpoint=/payment status=503 latency_ms=812 zone=161
val brut = spark.readStream
  .option("maxFilesPerTrigger", 3)      // 3 fichiers par micro-batch : plusieurs micro-batches, même avec --all
  .text("stream/logs")

// ----- 2. Parser avec regexp_extract ------------------------------------------------------
// Colonnes : ts (to_timestamp(..., "yyyy-MM-dd'T'HH:mm:ss.SSS")), niveau, endpoint, status (int), latence_ms (int)
val motif = """^(\S+) +(\w+) +endpoint=(\S+) status=(\d+) latency_ms=(\d+) zone=(\d+)$"""
val logs: DataFrame = ???   // brut.select(to_timestamp(regexp_extract(col("value"), motif, 1), "...").as("ts"), regexp_extract(...).as("niveau"), ...)

// ----- 3. Watermark de 2 minutes, fenêtre de 1 minute par endpoint ------------------------
// Dans agg : requetes (count), erreurs (sum(when(status >= 500, 1).otherwise(0))),
// latence_p95 (expr("percentile_approx(latence_ms, 0.95)")) ; PUIS taux_erreur = erreurs / requetes avec withColumn.
val parMinute: DataFrame = ???   // logs.withWatermark(...).groupBy(window(...), ...).agg(...).withColumn("taux_erreur", ...)

// ----- 4. Écrire en mode APPEND vers Parquet (out/tp10/par_minute), avec un checkpoint ---
// En direct : Trigger.ProcessingTime("5 seconds") et awaitTermination(120000). Avec --all : Trigger.AvailableNow().
Seq("out/checkpoints/tp10", "out/tp10").foreach(vider)      // repartir de zéro
val q: StreamingQuery = ???

// ----- 5. Lire le Parquet (en batch) et afficher les alertes : taux d'erreur > 10 % ------
// Quel endpoint ? À quelles minutes ? Que devient la latence p95 ?
???

// ----- 6. Combien de lignes ont été ignorées par le watermark ? ---------------------------
// Indice : q.recentProgress, champ stateOperators(0).numRowsDroppedByWatermark
// Pourquoi les toutes dernières minutes n'apparaissent-elles pas dans le Parquet ?
// À noter : lastProgress affiche le watermark en UTC (…T13:27:55Z pour 8h27 à New York), et une fenêtre de 7h52
// peut apparaître : ce sont des lignes arrivées avec 8 minutes de retard au tout début du flux.
???

// ----- Bonus – Fenêtre glissante de 5 minutes, toutes les minutes : l'alerte est-elle plus stable ? -----
// Indice : window(col("ts"), "5 minutes", "1 minute") sur spark.read.text("stream/logs") (en batch, c'est plus simple).
???
