// =====================================================================================
//  TP 1 – Premier contact avec Spark 4 (30 min)
// =====================================================================================
//  Fil rouge : un mois de courses de taxis jaunes de New York (data/trips.csv, ~120 000 lignes, 19 colonnes).
//  Objectif  : une SparkSession qui fonctionne, charger les données et lire la Spark UI.
//  Réussi si : 120 000 courses comptées, le schéma affiché, et vous savez retrouver vos jobs dans la Spark UI.
//
//  Comment travailler : ouvrez ce fichier dans VS Code, et spark-shell dans un terminal, À LA RACINE DU DÉPÔT
//  (les chemins sont relatifs à data/). Complétez chaque étape (remplacez les ???), puis collez-la dans
//  spark-shell : un bloc de plusieurs lignes se colle avec   :paste   puis Ctrl+D.
//  Pour tout rejouer d'un coup :   :load jour1/TP1_premier_contact.scala
// =====================================================================================

// ----- 1. La SparkSession -------------------------------------------------------------
// spark-shell l'a créée pour vous : `spark` (SparkSession) et `sc` (SparkContext).
// Affichez la version de Spark et l'adresse de la Spark UI (sc.uiWebUrl).
println(s"Spark ${spark.version} — Spark UI : ${???}")
// Dans une application compilée, vous l'écririez :
//   val spark = SparkSession.builder().appName("TP1").master("local[*]").getOrCreate()

// ----- 2. Charger data/trips.csv (avec en-tête) et compter les lignes -----------------
// Indice : spark.read.option("header", "true").option("inferSchema", "true").csv(chemin), puis .count()
val df = ???
println(s"Nombre de courses : ${???}")

// ----- 3. Afficher le schéma et 5 lignes ----------------------------------------------
// Indice : df.printSchema(), puis df.select("tpep_pickup_datetime", "PULocationID", "trip_distance", "total_amount").show(5)
???

// ----- 4. Ouvrir la Spark UI (adresse de l'étape 1), onglet Jobs ----------------------
// - Combien de jobs voyez-vous ? inferSchema = true relit le fichier pour deviner les types :
//   la lecture crée elle-même des jobs, avant votre count().
// - Cliquez sur le job du count() : il a 2 stages (un compte par partition, puis l'addition des comptes).
//   Combien de tâches dans le premier ? Comparez avec df.rdd.getNumPartitions.
// - Port 4040 déjà pris (un autre spark-shell ouvert) ? Spark passe à 4041 : le bon lien est sc.uiWebUrl.
println(s"Partitions : ${???}")

// ----- Bonus – CSV ou Parquet ? -------------------------------------------------------
// Chronométrez un count() sur data/trips.csv puis sur data/trips.parquet. Lequel est le plus rapide, et pourquoi ?
def chrono[T](bloc: => T): Double = { val t = System.nanoTime; bloc; (System.nanoTime - t) / 1e9 }
val tCsv = chrono(???)
val tPq  = chrono(???)
println(f"CSV : $tCsv%.2f s — Parquet : $tPq%.2f s")
