// Viz – « Spark agrège, le navigateur dessine » : des graphiques Plotly.js écrits en HTML depuis Scala.
// Dans spark-shell :   :load scripts/Viz.scala
// Puis : Viz.bar(df, "x", "y", "Titre", "out/tp5/1.html")  → ouvrir le fichier dans un navigateur.
// Plotly.js est fourni dans scripts/plotly.min.js (aucun accès internet nécessaire).
import org.apache.spark.sql.DataFrame

object Viz {
  /** scripts/plotly.min.js s'il existe (chemin relatif au fichier écrit), sinon le CDN. */
  private def scriptPlotly(sortie: java.nio.file.Path): String = {
    val local = java.nio.file.Paths.get("scripts/plotly.min.js").toAbsolutePath
    if (java.nio.file.Files.exists(local)) sortie.getParent.relativize(local).toString.replace("\\", "/")
    else "https://cdn.plot.ly/plotly-2.35.2.min.js"
  }

  private def js(v: Any): String = v match {
    case null => "null"
    case s: String => "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    case n: Number => n.toString
    case other => "\"" + other.toString + "\""
  }

  /** Une colonne d'un (petit) DataFrame → tableau JavaScript. */
  def col(df: DataFrame, c: String): String =
    df.select(c).collect().map(r => js(r.get(0))).mkString("[", ",", "]")

  def html(fichier: String, titre: String, traces: Seq[String], layout: String = ""): Unit = {
    val p = java.nio.file.Paths.get(fichier).toAbsolutePath
    val page =
      s"""<!doctype html><html><head><meta charset="utf-8"><title>$titre</title><script src="${scriptPlotly(p)}"></script></head>
         |<body><div id="g" style="width:900px;height:520px"></div>
         |<script>Plotly.newPlot("g", [${traces.mkString(",")}], Object.assign({title: ${js(titre)}}, {$layout}));</script>
         |</body></html>""".stripMargin
    java.nio.file.Files.createDirectories(p.getParent)
    java.nio.file.Files.write(p, page.getBytes("UTF-8"))
    println(s"Graphique écrit : $p")
  }

  def bar(df: DataFrame, x: String, y: String, titre: String, fichier: String): Unit =
    html(fichier, titre, Seq(s"""{type:"bar", x:${col(df, x)}, y:${col(df, y)}, marker:{color:"#3494BA"}}"""),
         s"""xaxis:{title:${js(x)}, type:"category"}, yaxis:{title:${js(y)}}""")

  def scatter(df: DataFrame, x: String, y: String, titre: String, fichier: String, mode: String = "markers",
              layout: String = ""): Unit =
    html(fichier, titre, Seq(s"""{type:"scatter", mode:${js(mode)}, x:${col(df, x)}, y:${col(df, y)},
                               | marker:{size:4, opacity:0.35, color:"#3494BA"}}""".stripMargin),
         s"""xaxis:{title:${js(x)}}, yaxis:{title:${js(y)}}$layout""")

  /** Heatmap : une ligne par valeur de y, une colonne par valeur de x, couleur = z (df déjà agrégé). */
  def heatmap(df: DataFrame, x: String, y: String, z: String, titre: String, fichier: String,
              etiquettesY: Seq[String] = Nil): Unit = {
    val pivot = df.groupBy(y).pivot(x).sum(z).na.fill(0).orderBy(y)
    val xs = pivot.columns.tail
    val rows = pivot.collect()
    val ys: Seq[String] = if (etiquettesY.nonEmpty) etiquettesY.map(js) else rows.toSeq.map(r => js(r.get(0)))
    val matrice = rows.map(r => xs.indices.map(i => js(r.get(i + 1))).mkString("[", ",", "]")).mkString("[", ",", "]")
    html(fichier, titre, Seq(s"""{type:"heatmap", x:${xs.map(js).mkString("[", ",", "]")}, y:${ys.mkString("[", ",", "]")},
                               | z:$matrice, colorscale:[[0,"#F1ECE7"],[0.5,"#58B6C0"],[1,"#1F4E79"]]}""".stripMargin),
         s"""xaxis:{title:${js(x)}, type:"category"}""")
  }
}
