// TP 1 – Premier contact avec Spark 4 (corrigé Scala)
// Dans spark-shell, lancé depuis la racine du dépôt :   :load jour1/corriges/tp1_premier_contact.scala

// %% 1. La SparkSession : spark-shell l'a créée pour vous (`spark`), ainsi que le SparkContext (`sc`)
println(s"Spark ${spark.version} — Spark UI : ${sc.uiWebUrl.getOrElse("?")}")
// Dans une application compilée (spark-submit), on l'écrirait soi-même :
//   val spark = SparkSession.builder().appName("TP1").master("local[*]").getOrCreate()

// %% 2. Charger le fil rouge et compter les lignes
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/trips.csv")
println(s"Nombre de courses : ${df.count()}")

// %% 3. Schéma et 5 lignes
df.printSchema()
df.select("tpep_pickup_datetime", "PULocationID", "trip_distance", "total_amount").show(5)

// %% 4. Combien de partitions ? (regarder aussi l'onglet Jobs de la Spark UI)
println(s"Partitions : ${df.rdd.getNumPartitions}")

// %% Bonus – CSV ou Parquet : qui lit le plus vite ?
def chrono[T](bloc: => T): Double = { val t = System.nanoTime; bloc; (System.nanoTime - t) / 1e9 }
val tCsv = chrono(spark.read.option("header", "true").option("inferSchema", "true").csv("data/trips.csv").count())
val tPq  = chrono(spark.read.parquet("data/trips.parquet").count())
println(f"CSV (avec inférence du schéma) : $tCsv%.2f s — Parquet : $tPq%.2f s")
// Parquet est plus rapide : format colonne, compressé, avec le schéma embarqué (pas d'inférence).
