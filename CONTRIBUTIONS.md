# Journal de contribution — GROUPE 3

## 1. Qui a fait quoi

| Question | Sujet | Responsable | Relecteur |
|---|---|---|---|
| 1.1 – 1.3 | Structure SBT, build.sbt, README | A | C |
| 2.1 – 2.5 | Ingestion, validation, rapport de qualité, orphelins | A | B |
| 3.1 – 3.4 | UDF, enrichissement, fenêtres, transactions suspectes | B | A |
| 4.1 – 4.4 | KPI marchands, cohortes, RFM, produits | C | B |
| 5.1 – 5.3 | Cache/persist, broadcast, mesure des gains | C | A et B |
| 6.1 – 6.2 | Application principale, exécution par étape | C | A, B (intégration validée par les 3) |
| 7.1 | application.conf | A | C |

## 2. Charge de travail et difficultés (à remplir honnêtement par chaque membre)

| Membre | Heures estimées | Difficultés rencontrées |
|---|---:|---|
| A | | |
| B | | |
| C | | |

## 3. Décisions techniques du groupe

* **Versions** : Spark 3.5.3 et Scala 2.12.18 (Spark 3.5 est publié pour Scala 2.12 et 2.13 ; 2.12 est la version la plus répandue avec cette branche de Spark). SBT 1.9.9, Java 11/17.
* **JAR exécutable : `sbt-assembly`** plutôt que `package`. `package` ne contient que nos classes : Typesafe Config
  manquerait à l'exécution. `assembly` produit un JAR autonome contenant notre code et Typesafe Config ; Spark reste
  `provided` (fourni par `spark-submit`), ce qui garde le JAR léger et évite les conflits de versions avec le cluster.
* **Format de sortie** : CSV (en-tête, `coalesce(1)`) et Parquet, mode overwrite, dans `output/csv/<nom>` et
  `output/parquet/<nom>`. Le CSV convertit tableaux (`concat_ws`) et structures (`to_json`) en chaînes. Le rapport
  de qualité est un CSV unique renommé depuis `part-*.csv`.
* **Stratégie de jointure** (Q3.2) :

| Table jointe | Type | Justification |
|---|---|---|
| users | left | Une transaction valide dont l'utilisateur est inconnu ou rejeté doit rester dans l'analyse (chiffre d'affaires conservé) ; ses colonnes users sont nulles. |
| products | left | Même raison : produit orphelin ou rejeté (prix, note invalides) ; on garde la vente. |
| merchants | left | Même raison ; les KPI marchands filtrent ensuite explicitement les marchands inconnus. |

  Un `inner` aurait supprimé jusqu'à ~1 000 transactions (orphelins + références rejetées) et faussé les totaux.
  Les trois tables de référence sont diffusées (`broadcast`) car elles sont petites (12 000, 6 000 et 600 lignes).
* **Validation** : un timestamp valide doit avoir exactement 14 chiffres (14 caractères, tous numériques) ; toutes les
  raisons de rejet violées sont concaténées avec « | ». « NA » est converti en null à la lecture.
* **Cas laissés libres par le sujet** : l'âge 25 est rattaché à « Adulte » ; `is_working_hours` inclut 9h et 17h ;
  règles RFM détaillées dans le README ; orphelins comptés avant validation.
* **Optimisations** : `cache()` sur sources et résultats réutilisés, `persist(MEMORY_AND_DISK_SER)` sur le DataFrame
  enrichi (le plus volumineux), `unpersist()` en fin de pipeline, `spark.sql.shuffle.partitions` lu dans la configuration.

## 4. Relectures croisées (une entrée datée par module)

| Date | Module | Auteur | Relecteur | Remarques |
|---|---|---|---|---|
| | DataIngestion / DataValidation | A | B | |
| | TimeFeatures / DataTransformation | B | A | |
| | Analytics / SparkOptimizations / MainApp | C | B (Analytics), A et B (Optimisations) | |
| | build.sbt / README / application.conf | A | C | |
