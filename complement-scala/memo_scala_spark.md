# Mémo Scala pour Spark (complément « Scala de A à Z »)

| Besoin | Scala | Rappel |
|---|---|---|
| Valeur / variable | `val x = 1` / `var y = 2` | `val` partout ; une `var` modifiée dans une lambda ne revient pas du cluster |
| Fonction | `def f(a: Int, b: Double = 1.0): Double = …` | types des paramètres obligatoires ; pas de `return` |
| Lambda | `x => x * 2`, `_ * 2`, `_ + _`, `(a: Int, b: Int) => a + b` | `_` = le paramètre, utilisé une fois, dans l'ordre |
| Chaînes | `s"$x ${y + 1}"`, `f"$d%.2f $$"`, `"""multi"""` | `$$` pour un dollar littéral |
| Collections | `List`, `Vector`, `Set`, `Map(k -> v)`, `Array` (mutable) | `:+` ajoute (nouvelle collection), `::` ajoute devant une List |
| Méthodes | `map filter flatMap groupBy sortBy reduce foldLeft zip take mkString` | mêmes noms sur les RDD |
| Tuples | `(1, "a")`, `t._1`, `val (a, b) = t`, `case (a, b) =>` | RDD de tuples = PairRDD (`reduceByKey`, `join`) |
| Option | `Some(v)` / `None`, `getOrElse`, `map`, `match` | `Map.get`, `headOption`, `args.headOption` |
| Erreurs | `Try(expr)`, `Success` / `Failure`, `.toOption`, `Either[L, R]` | `flatMap(s => Try(s.toInt).toOption)` nettoie un CSV |
| Pattern matching | `x match { case 0 => … ; case n: Int if n > 1 => … ; case (a, b) => … ; case _ => … }` | une expression ; `sealed` ⇒ exhaustivité vérifiée |
| Données | `case class Course(zone: Int, montant: Double)` | `copy`, `==` par valeur, déstructuration, `Dataset[Course]` |
| Classes | `class C(val a: Int)`, `object O { def apply(…) }`, `trait T { def f: Int; def g = … }` | `extends A with B` ; `Nom(args)` = `Nom.apply(args)` |
| Génériques | `def premier[T](xs: List[T]): T`, `RDD[T]`, `Dataset[T]` | `[T: Ordering]` : T doit être ordonnable |
| Implicites | `import spark.implicits._` (après la création de `spark`) | `$"col"`, `toDF`, `.as[T]`, Encoders |
| Colonnes | `col("x")`, `$"x"`, `expr("x * 2")`, `df("x")` ; `===`, `=!=`, `&&`, `\|\|`, `!` | jamais `==` entre colonnes |
| Spark ML | `new StringIndexer().setInputCol("a").setOutputCol("b")`, `Array[PipelineStage]`, `prep :+ modele` | `stages.last.asInstanceOf[…Model]` |
| Streaming | `Trigger.ProcessingTime("5 seconds")`, `awaitTermination(60000)` (ms !), `foreachBatch(traiter _)` | `def traiter(b: DataFrame, id: Long): Unit` |
| Projet | `scalaVersion := "2.13.16"`, `"org.apache.spark" %% "spark-sql" % v % "provided"` | pas de `.master()` dans le code ; `spark-submit --class` |
| REPL | `spark-shell`, `:paste` + Ctrl+D, `:load f.scala`, `:type expr`, `:quit` | une `case class` chargée par `-i` peut bloquer `.as[T]` : la déclarer au prompt |
