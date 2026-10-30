# scala-app – l'application Spark du fil rouge (sbt)

Projet de référence du complément Scala (TP B2 et B3) : le top 10 des zones et le modèle de pourboire sur le flux,
sous forme d'application compilée, testée et déployable par `spark-submit`.

```bash
cd scala-app
sbt compile                         # 1er lancement : téléchargement des dépendances (Maven Central)
sbt test                            # ScalaTest, SparkSession local[2]
sbt package                         # → target/scala-2.13/formation-spark-app_2.13-0.1.0.jar
spark-submit --class fr.ambientit.spark.Top10Zones --master "local[*]" \
    target/scala-2.13/formation-spark-app_2.13-0.1.0.jar ../data
```

Alignez `sparkVersion` dans `build.sbt` sur `spark-shell --version`.

## Structure

- `Transformations.scala` : les fonctions métier, `DataFrame => DataFrame`, pures et testées (`caParZone`, `avecNomZone`, `featuresPourboire`).
- `Top10Zones.scala` : `main` du TP B2 (lecture, transformations, affichage). Pas de `.master()` : il vient de `spark-submit`.
- `PourboireStream.scala` : `main` du TP B3 : le modèle du TP 7 (`out/modele_pourboire`) appliqué au flux `stream/courses`
  avec watermark, fenêtres de 30 min et `foreachBatch`. Arguments : `data stream/courses out/modele_pourboire out/appli` ; `MODE=availableNow` pour un run reproductible.
- `TransformationsSpec.scala` : deux tests (agrégation, features du pourboire) sur des DataFrames de 3 lignes.

## TP B2 – Empaqueter le top 10 (40 min)

1. `sbt compile`, puis lire `Top10Zones` et `Transformations`.
2. Ajouter `caParHeure(df: DataFrame): DataFrame` dans `Transformations` : courses et CA par heure de prise en charge
   (`hour(col("tpep_pickup_datetime"))`, `groupBy("heure").agg(count("*"), round(sum("total_amount"), 0))`).
3. Un test dans `TransformationsSpec` : 3 lignes sur 2 heures → les bons comptes
   (`java.sql.Timestamp.valueOf("2024-01-15 18:30:00")` pour la colonne horodatage).
4. Un `object ParHeure` avec `main` : afficher les 3 meilleures heures (`orderBy(desc("ca")).show(3)`).
5. `sbt package`, puis `spark-submit --class fr.ambientit.spark.ParHeure … ../data`.

**Réussi si** : `sbt test` vert, et 18h en tête (≈ 237 600 $).
Bonus : le même JAR sur le cluster local (`bash ../jour1/TP3_cluster/start_cluster_local.sh`, `--master spark://127.0.0.1:7077`) ; `sbt-assembly`.

## TP B3 – Le modèle sur le flux, en application (45 min)

Prérequis : `out/modele_pourboire` (corrigé Scala du TP 7 : `spark-shell -i jour2/corriges/tp7_classification.scala`).

1. Lire `PourboireStream.scala` : repérer les arguments, le chargement du modèle, le watermark, `foreachBatch`.
2. Ajouter une alerte dans `traiter` : les fenêtres où `|taux_predit − taux_reel| > 0,25` sont affichées et écrites dans `<sortie>/alertes` (Parquet).
3. Extraire ce filtre dans `Transformations.alertes(batch: DataFrame): DataFrame` et le tester sur 2 fenêtres construites avec `Seq(...).toDF`.
4. `sbt package` ; dans un autre terminal `python scripts/stream_simulator.py courses --reset` ; puis
   `spark-submit --class fr.ambientit.spark.PourboireStream <jar> ../data ../stream/courses ../out/modele_pourboire ../out/appli`.
5. Spark UI (port 4040), onglet *Structured Streaming* : débit d'entrée, durée des micro-batches.

**Réussi si** : prédictions en continu, test vert, au moins une alerte affichée.
Bonus : lire le flux en `Dataset[Course]` ; `sealed trait Alerte` avec deux niveaux (écart > 0,25 / > 0,4).

## Vérifié / non vérifié

`Top10Zones` et `PourboireStream` ont été compilés avec le compilateur Scala livré avec Spark 4.2.0 et exécutés par `spark-submit`
(résultats identiques aux corrigés). Le build sbt et le test ScalaTest n'ont pas pu être exécutés dans l'environnement de
préparation (pas d'accès à Maven Central) : lancez `sbt test` sur votre poste avant la session.
