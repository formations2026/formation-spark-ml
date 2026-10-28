// =====================================================================================
//  TP 9 – Votre premier stream (35 min)
// =====================================================================================
//  Objectif  : suivre en direct le chiffre d'affaires et le nombre de courses par arrondissement.
//  Réussi si : le tableau se met à jour à chaque micro-batch.
//
//  AVANT DE COMMENCER, dans un terminal à la racine du dépôt :
//    python scripts/stream_simulator.py courses --reset          # en direct : un fichier toutes les 2 secondes
//    python scripts/stream_simulator.py courses --all --reset    # en retard / mode dégradé : tout d'un coup
// =====================================================================================
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
spark.conf.set("spark.sql.shuffle.partitions", "4")     // petit volume : 200 partitions par défaut, c'est trop
def vider(d: String): Unit = new scala.reflect.io.Directory(new java.io.File(d)).deleteRecursively()

// ----- 1. Lire le dossier stream/courses en continu ------------------------------------
// Un stream exige un SCHÉMA EXPLICITE : prenez celui de data/trips.parquet (spark.read.parquet(...).schema).
// Indices : spark.readStream.schema(schema).option("maxFilesPerTrigger", 2).json("stream/courses"), puis .isStreaming
val schema:  StructType = ???
val courses: DataFrame  = ???
println(courses.isStreaming)

// ----- 2. Jointure stream-statique avec les zones --------------------------------------
// Le référentiel data/taxi_zone_lookup.csv est un DataFrame STATIQUE ordinaire. Ajoutez la colonne Borough.
val zones:     DataFrame = ???
val enrichies: DataFrame = ???

// ----- 3. Chiffre d'affaires et nombre de courses par Borough ---------------------------
val parBorough: DataFrame = ???

// ----- 4. Démarrer la requête ------------------------------------------------------------
// outputMode("complete"), format("console"), trigger(Trigger.ProcessingTime("5 seconds")), un checkpointLocation.
// Laissez tourner 60 secondes : q.awaitTermination(60000), puis q.stop(). Un tableau s'affiche par micro-batch.
// ⚠ Le checkpoint est la MÉMOIRE de la requête : si vous relancez après un --reset du simulateur, supprimez-le
//   d'abord (vider(...)), sinon la requête reprend l'ancien état et ignore les nouveaux fichiers.
vider("out/checkpoints/tp9")
val q: StreamingQuery = ???   // parBorough.writeStream.outputMode(...).format(...).trigger(...).option("checkpointLocation", "out/checkpoints/tp9").start()
q.awaitTermination(60000)
q.stop()

// ----- 5. Le suivi : q.lastProgress, q.recentProgress ------------------------------------
// Combien de micro-batches ? Combien de lignes par micro-batch (numInputRows) ?
???

// ----- Bonus – Le même calcul en BATCH (spark.read) sur les mêmes fichiers : mêmes comptes ? -----
// Si le stream en a moins : comparez le rythme du simulateur (1 fichier / 2 s) et maxFilesPerTrigger (2 fichiers
// par micro-batch de 5 s). Le stream a-t-il pris du retard ? Que se passe-t-il si on le laisse tourner plus longtemps ?
???
