# Formation Spark 4 & Machine Learning – dépôt des travaux pratiques

> **Branches** : `main` = `scala` (TP en **Scala**, langage par défaut du programme, + complément « Scala de A à Z ») ·
> [`pyspark`](../../tree/pyspark) (les mêmes TP en **Python**, notebooks Jupyter) ·
> [`participants`](../../tree/participants) (énoncés seuls).

Support des TP de la formation **Spark 4 & Machine Learning** (3 jours, Ambient IT).
Un seul projet **fil rouge** sur les 3 jours : les courses de taxis jaunes de New York.

> **Jour 1 – Comprendre et utiliser Spark 4** : *quelles zones de New York rapportent le plus, et à quelle heure ?*
> Résolu 3 fois, pour comparer : avec les RDD, sur un cluster + HDFS, puis en Spark SQL.
>
> **Jour 2 – Machine Learning** : *combien va coûter cette course ? Ce client laissera-t-il un pourboire ? Quelles familles de courses ?*
> Régression, classification, clustering, sur le même jeu de données.
>
> **Jour 3 – Streaming** : *voir le chiffre d'affaires en direct, détecter une panne dans les logs, prédire le pourboire en temps réel.*
> `scripts/stream_simulator.py` fait arriver courses et logs fichier par fichier, comme en production.

## Démarrage rapide (participants)

```bash
git clone <URL_DU_DEPOT> formation-spark-ml
cd formation-spark-ml
python -m venv .venv && source .venv/bin/activate      # Windows : .venv\Scripts\activate
pip install -r requirements.txt
python scripts/check_env.py                              # Java 17+, PySpark 4, Jupyter, Git, SparkSession
```

Puis, **depuis la racine du dépôt**, ouvrez `jour1/TP1_premier_contact.scala` dans VS Code et `spark-shell` dans un terminal :

```bash
spark-shell                                   # Scala 2.13, Spark 4 : `spark` et `sc` sont prêts
scala> :load jour1/TP1_premier_contact.scala  # ou collez les étapes une à une (:paste puis Ctrl+D)
```

`spark-shell` est installé par `pip install pyspark` (dossier `bin/` du paquet, aussi en `.cmd` sous Windows) ;
une distribution Spark classique (`spark-4.x-bin-hadoop3.tgz`) convient aussi.

## Scala par défaut, Python en option

Conformément au programme, les TP sont en **Scala** : énoncés `jourN/TPx_*.scala` (à compléter, les `???` sont typés),
corrigés `jourN/corriges/*.scala`, à charger dans `spark-shell`. La version **Python** (notebooks Jupyter, mêmes
étapes, mêmes résultats) est dans `jourN/python/` pour les participants qui le préfèrent : un groupe mixte est possible,
les deux versions partagent les données, les scripts et les résultats attendus.

Prérequis détaillés : [docs/prerequis.md](docs/prerequis.md). **Sous Windows, utilisez WSL2.**

> Les dossiers `corriges/` sont publiés à la fin de chaque journée : essayez d'abord par vous-même !

## Contenu

```
data/                      fil rouge : trips.csv (120 000 courses), trips.parquet, taxi_zone_lookup.csv
jour1/                     Comprendre et utiliser Spark 4
  TP1_premier_contact.scala        30 min   SparkSession, chargement, Spark UI
  TP2_rdd_top10_zones.scala        30 min   RDD, PairRDD, reduceByKey, join, stages, SparkListener
  TP3_cluster/                     50 min   cluster Standalone (local ou Docker) + HDFS
  TP4_spark_sql.scala              45 min   DataFrames, schéma, SQL, partitionBy, explain, ANSI, Dataset typé
  corriges/                        corrigés Scala (:load dans spark-shell)
  python/                          la même chose en Python : notebooks + corriges/ (.ipynb et .py)
jour2/                     Machine Learning avec Spark ML
  TP5_explorer.scala               35 min   visualiser avant de modéliser (Spark agrège, Plotly dessine)
  TP6_regression.scala             30 min   Pipeline, régression linéaire vs arbre, RMSE / R², persistance
  TP7_classification.scala         50 min   Random Forest vs SVM, matrice de confusion, ROC / AUC
  TP8_clustering.scala             45 min   K-means + silhouette, bonus GraphFrames
  corriges/, python/
jour3/                     Spark Streaming et Spark à grande échelle
  TP9_premier_stream.scala         35 min   Structured Streaming, jointure stream-statique, output modes
  TP10_logs_temps_reel.scala       35 min   analyse de logs en temps réel : fenêtres, watermark, append
  TP11_ml_streaming_catalyst.scala 55 min   modèle ML sur un flux, foreachBatch, Catalyst, Tungsten, AQE
  corriges/, python/
quiz/                      quiz des 3 jours (avec réponses) et correction de l'atelier Lambda / Kappa
formateur/                 checklists des 3 jours, mode dégradé, mail de prérequis, publier sans les corrigés
scripts/                   Viz.scala (graphiques Plotly depuis Scala), generate_data.py, download_official.py,
                           stream_simulator.py, check_env.py, build_notebooks.py
complement-scala/          module « Scala de A à Z » (2 jours, 90 slides) : TP A1–A3, B1 + corrigés, mémo
scala-app/                 l'application Spark du fil rouge : projet sbt, tests ScalaTest, spark-submit (TP B2, B3)
```

## Complément « Scala de A à Z »

Pour les groupes qui découvrent Scala, un module de 2 jours précède la formation : du premier `val` à une application
Spark compilée qui traite un flux. Support `Formation01-SparkML_Complement-Scala-A-Z.pptx`, TP dans `complement-scala/`
et `scala-app/`.

## Les données

`data/` contient un jeu **synthétique** au format exact des « Yellow Taxi Trip Records » de la NYC Taxi & Limousine
Commission (19 colonnes, mêmes types, mêmes ordres de grandeur, 1 mois, 120 000 courses) : assez petit pour un
portable et pour Git, assez réaliste pour que les résultats aient du sens (JFK et LaGuardia en tête du chiffre d'affaires).

Les 5 colonnes utilisées au jour 1 :

| Colonne | Signification | Index CSV |
|---|---|---|
| `tpep_pickup_datetime` | date et heure de prise en charge (heure locale, sans fuseau) | 1 |
| `PULocationID` | zone de départ (1 à 265) | 7 |
| `trip_distance` | distance en miles | 4 |
| `total_amount` | montant total payé ($) | 16 |
| `taxi_zone_lookup.csv` | LocationID → Borough, Zone | – |

Pour utiliser les **données réelles** (un mois complet ≈ 3 millions de courses) :
`python scripts/download_official.py --year 2024 --month 1 --rows 300000`

## Versions

Testé avec Spark **4.2.0** (Scala 2.13.18, `spark-shell` du paquet PySpark 4.2.0), Java 21, Python 3.13.
Fonctionne avec toute version 4.x de Spark et Java 17+. Les corrigés Scala et Python donnent les mêmes résultats
(à ±0,01 près sur les AUC : l'ordre des lignes après jointure, donc le tirage de `randomSplit`, diffère).

Deux pièges propres à `spark-shell` : une `case class` déclarée dans un script chargé peut bloquer l'exécution
(le TP 4 utilise un tuple typé ; dans un projet sbt, `.as[Course]` est la bonne pratique) ; les accents s'affichent
« ? » si le terminal n'est pas en UTF-8 (Windows : `chcp 65001`).
Pour le TP 3 en conteneurs, aligner la version de l'image `apache/spark` sur celle de `pip show pyspark`.

## Licence

Contenu pédagogique © Ambient IT. Données synthétiques générées par `scripts/generate_data.py`.
Les noms de zones proviennent du référentiel public de la NYC TLC.
