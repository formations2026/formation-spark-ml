# Complément « Scala de A à Z » (2 jours, 90 slides)

Module complémentaire à la formation Spark 4 & Machine Learning : du niveau débutant (jamais écrit de Scala)
à une application Spark compilée, testée, qui traite un flux. Support : `Formation01-SparkML_Complement-Scala-A-Z.pptx`.

| TP | Durée | Fichier | Corrigé |
|---|---|---|---|
| A1 – Premiers pas dans le REPL | 25 min | `TP_A1_premiers_pas.scala` | `corriges/tp_a1.scala` |
| A2 – Top 10 des zones en Scala pur | 35 min | `TP_A2_top10_scala_pur.scala` | `corriges/tp_a2.scala` |
| A3 – Modéliser le fil rouge | 35 min | `TP_A3_modeliser.scala` | `corriges/tp_a3.scala` |
| B1 – Le fil rouge en Dataset typé | 35 min | `TP_B1_dataset_type.scala` | `corriges/tp_b1.scala` |
| B2 – Empaqueter le top 10 en application | 40 min | `../scala-app/README.md` | `../scala-app/` |
| B3 – Le modèle sur le flux, en application | 45 min | `../scala-app/README.md` | `../scala-app/` |

Tout se fait dans `spark-shell` (REPL Scala 2.13 livré avec Spark) depuis la racine du dépôt, puis dans `scala-app/` avec sbt.
Fiche mémo : `memo_scala_spark.md`.
