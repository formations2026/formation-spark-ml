// TP 3 – Le TP 2 sur un cluster Standalone, données lues depuis HDFS (ou en local)
//
//   spark-shell --master spark://127.0.0.1:7077 -i jour1/TP3_cluster/tp2_cluster.scala
//   DATA=hdfs://namenode:8020/data spark-shell --master spark://spark-master:7077 -i jour1/TP3_cluster/tp2_cluster.scala
//
// Le code métier est IDENTIQUE au TP 2 : seuls --master et le chemin des données changent.
val base = sys.env.getOrElse("DATA", "data")
println(s"Master : ${sc.master} — données : $base — UI : ${sc.uiWebUrl.getOrElse("?")}")

val lignes = sc.textFile(s"$base/trips.csv")
val entete = lignes.first()
val paires = lignes.filter(_ != entete).map(_.split(",")).map(c => (c(7).toInt, c(16).toDouble))
val top10 = paires.reduceByKey(_ + _).takeOrdered(10)(Ordering.by[(Int, Double), Double](-_._2))

val zl = sc.textFile(s"$base/taxi_zone_lookup.csv")
val zlEntete = zl.first()
val zones = zl.filter(_ != zlEntete).map(_.split(",").map(_.replace("\"", ""))).map(c => (c(0).toInt, s"${c(2)} (${c(1)})"))
val resultat = sc.parallelize(top10.toSeq).join(zones).sortBy(-_._2._1).collect()
println("\nTop 10 des zones de départ par chiffre d'affaires :")
resultat.zipWithIndex.foreach { case ((_, (ca, nom)), i) => println(f"${i + 1}%2d. $nom%-45s $ca%,12.2f $$") }
println("Regardez l'UI du master (http://localhost:8080) : votre application, ses executors, ses cœurs.")
