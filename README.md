# EcommerceAnalytics — Système d'analyse de données e-commerce (Spark & Scala)

Projet final Data Engineer – Spark & Scala (GROUPE 3). Pipeline distribué : ingestion de 4 sources
(CSV, JSON, Parquet), validation avec rapport de qualité, enrichissement par jointures et fonctions de fenêtrage,
indicateurs métier, optimisations Spark, écriture des résultats en CSV et Parquet.

## 1. Prérequis

| Outil | Version testée | Remarque |
|---|---|---|
| Java (JDK) | 11 ou 17 (21 fonctionne) | Java 8 fonctionne aussi (les options `--add-opens` sont alors omises) |
| Scala | 2.12.18 | Téléchargée automatiquement par SBT |
| SBT | 1.9.x | https://www.scala-sbt.org/download |
| Spark | 3.5.3 (Scala 2.12) | Seulement pour `spark-submit` ; inutile pour `sbt run` (Spark est tiré par SBT) |

Vérification : `java -version` et `sbt --version`.

### Windows (IntelliJ / sbt) — points importants

- **Dans le shell sbt d'IntelliJ**, on tape `run` ou `run all` (et non `sbt run`, qui donne « Not a valid command: sbt »).
  Depuis un terminal Windows : `sbt run`.
- **Écriture des fichiers** : Spark/Hadoop a besoin de `winutils.exe` et `hadoop.dll` sous Windows. Ils sont fournis dans
  `hadoop/bin/` (Hadoop 3.3.5) et configurés automatiquement par `build.sbt` (`-Dhadoop.home.dir`, `-Djava.library.path`)
  et par `WindowsSupport.init()`. Sans cela : `UnsatisfiedLinkError: NativeIO$Windows.createDirectoryWithMode0`.
  Si vous lancez `MainApp` directement depuis IntelliJ (hors sbt), ajoutez dans *VM options* :
  `-Dhadoop.home.dir=<chemin du projet>\hadoop -Djava.library.path=<chemin du projet>\hadoop\bin`
  (plus les `--add-opens` de `build.sbt` pour Java 17+).
- **Accents illisibles** (`d├®but`) : `build.sbt` détecte la page de code de la console (`chcp`, 850 en France). Pour forcer :
  variable d'environnement `CONSOLE_ENCODING` (ex. `UTF-8` ou `Cp1252`), puis relancer sbt.
- Les logs Spark sont limités aux avertissements (`src/main/resources/log4j2.properties`).

## 2. Compilation et génération du JAR

```bash
sbt clean compile          # compilation
sbt test                   # tests unitaires (ScalaTest)
sbt assembly               # JAR exécutable -> target/scala-2.12/ecommerce-analytics.jar
```

Le JAR n'est plus fourni précompilé (l'ancien `dist/` correspondait à une version antérieure du code) : il se
régénère avec `sbt assembly`.

Spark est déclaré `provided` : il n'est pas embarqué dans le JAR (il est fourni par `spark-submit` / le cluster).
Typesafe Config est, lui, embarqué.

## 3. Exécution locale avec SBT

À lancer **depuis la racine du projet** (les chemins de `application.conf` sont relatifs ; à défaut de fichier
au chemin indiqué, `src/main/resources/<chemin>` est essayé, ce qui couvre `data/...`).

```bash
sbt run                         # pipeline complet (= all)
sbt "run ingestion"             # ingestion + validation + rapport de qualité + rejets
sbt "run transformation"        # ... + enrichissement, fenêtrage, transactions suspectes
sbt "run analytics"             # ... + KPI, cohortes, RFM, produits (= all)
sbt "run benchmark"             # pipeline sans puis avec optimisations (tableau §7)
sbt "run inconnu"               # affiche l'aide (arguments acceptés), sans exception
```

Chaque étape inclut celles dont elle dépend (la transformation a besoin des données validées, etc.).
Chaque étape journalise heure de début, heure de fin et durée.

N'importe quelle clé de `application.conf` se surcharge par une propriété JVM. Avec `sbt run` (JVM dédiée, `fork := true`) :

```bash
sbt 'set Compile / run / javaOptions += "-Dapp.optimization.enable-cache=false"' run
```

## 4. Déploiement avec spark-submit

```bash
# Local
spark-submit --class com.ecommerce.analytics.MainApp --master "local[*]" \
  target/scala-2.12/ecommerce-analytics.jar all

# Cluster (YARN) : données et sortie sur HDFS, chemins surchargés
spark-submit --class com.ecommerce.analytics.MainApp \
  --master yarn --deploy-mode cluster \
  --driver-memory 2g --executor-memory 4g --num-executors 4 \
  --conf "spark.driver.extraJavaOptions=-Dapp.data.input.transactions=hdfs:///ecom/transactions.csv -Dapp.data.input.users=hdfs:///ecom/users.json -Dapp.data.input.products=hdfs:///ecom/products.parquet -Dapp.data.input.merchants=hdfs:///ecom/merchants.csv -Dapp.data.output.path=hdfs:///ecom/output/" \
  ecommerce-analytics.jar all
```

Le `master` de `application.conf` (`local[*]`) est ignoré quand `--master` est fourni.
Un argument d'étape (`ingestion`, `transformation`, `analytics`, `all`, `benchmark`) s'ajoute après le JAR.

## 5. Structure du projet

```
EcommerceAnalytics/
├── build.sbt, project/           SBT, sbt-assembly
├── hadoop/bin/                   winutils.exe + hadoop.dll (Windows uniquement)
├── README.md, EQUIPE.md, CONTRIBUTIONS.md, .gitignore
├── src/main/scala/com/ecommerce/
│   ├── models/Models.scala               case classes Transaction, User, Product, Merchant   (Membre A)
│   ├── analytics/DataIngestion.scala     lecture CSV/JSON/Parquet, try-catch, comptage        (Membre A)
│   ├── analytics/DataValidation.scala    règles, rejets + rejection_reason, rapport qualité   (Membre A)
│   ├── analytics/TimeFeatures.scala      UDF extractTimeFeatures                              (Membre B)
│   ├── analytics/DataTransformation.scala jointures, fenêtres, suspects                       (Membre B)
│   ├── analytics/Analytics.scala         KPI marchands, cohortes, RFM, produits               (Membre C)
│   ├── analytics/SparkOptimizations.scala cache, persist, unpersist, broadcast                (Membre C)
│   ├── analytics/MainApp.scala           orchestration + gestion d'erreurs globale            (Membre C)
│   └── utils/  ConfigLoader, SparkSessionBuilder, DataFrameWriterUtils, PathUtils, StepTimer
├── src/main/resources/application.conf   configuration externalisée
├── src/main/resources/data/              transactions.csv, users.json, products.parquet, merchants.csv
├── src/test/scala/                       tests unitaires
└── output/                               résultats (csv/, parquet/, rapport_qualite_yyyymmdd.csv)
```

## 6. Résultats produits (dossier `output/`)

Tout résultat est écrit deux fois : `output/csv/<nom>/` (en-tête, un seul fichier `part-*.csv`) et
`output/parquet/<nom>/`, en mode overwrite, par une fonction unique (`DataFrameWriterUtils.writeCsvAndParquet`).
Le rapport de qualité est un CSV unique : `output/rapport_qualite_yyyymmdd.csv`.

| Sortie | Question | Statut |
|---|---|---|
| rapport_qualite_yyyymmdd.csv | 2.4 (+2.5) | obligatoire (+ colonnes bonus d'orphelins) |
| rejets_transactions / users / products / merchants | 2.2 | obligatoire |
| transactions_enrichies | 3.2 + 3.3 | obligatoire |
| kpi_marchands, cohortes_retention, meilleure_cohorte_3m | 4.1, 4.2 | obligatoire |
| transactions_suspectes | 3.4 | bonus |
| rfm_clients, rfm_x_customer_segment | 4.3 | bonus |
| top10_produits, ca_categorie_region, ca_paiement_periode | 4.4 | bonus |
| benchmark_optimisations.csv | 5.3 | bonus |

Rapport de qualité obtenu sur les données fournies :

| dataset | lues | valides | rejetées | taux_rejet (%) | valeurs nulles |
|---|---:|---:|---:|---:|---:|
| transactions | 138 047 | 136 157 | 1 890 | 1,37 | 1 076 |
| users | 12 000 | 11 655 | 345 | 2,88 | 160 |
| products | 6 000 | 5 821 | 179 | 2,98 | 55 |
| merchants | 600 | 586 | 14 | 2,33 | 9 |

Orphelins parmi les transactions lues : 400 `user_id`, 300 `product_id`, 250 `merchant_id`.
Le texte « NA » présent dans certaines cellules est traité comme une valeur manquante (null).

## 7. Mesure du gain des optimisations (Q5.3)

Mode « sans » : `enable-cache = false` et `enable-broadcast = false`. Mode « avec » : les deux à `true`
(`application.conf`, clés `app.optimization.*`). Commande : `sbt "run benchmark"`. Machine de test : 1 seul poste,
mode local, 138 047 transactions.

| Étape | Sans optimisation (ms) | Avec optimisation (ms) | Gain (%) |
|---|---:|---:|---:|
| ingestion | 8 321 | 3 628 | 56,4 |
| validation | 10 381 | 3 796 | 63,4 |
| transformation | 22 723 | 13 937 | 38,7 |
| analytique | 61 733 | 15 340 | 75,2 |
| écriture | 77 716 | 10 599 | 86,4 |
| **Total** | **180 874** | **47 300** | **73,8** |

Lecture des résultats et limites : sans cache, chaque action (comptage, affichage, écriture CSV, écriture Parquet)
recalcule tout le plan, d'où le coût élevé de l'écriture et de l'analytique. Les deux exécutions ont lieu dans la
même JVM et la seconde profite de la JVM déjà « chaude » et du cache disque du système : une part du gain
vient de ce biais, pas uniquement de `cache`/`broadcast`. Les durées sont à refaire sur votre machine avant la soutenance.

## 8. Choix de conception à connaître

* **Jointures** : toutes en `left` depuis les transactions valides (aucun chiffre d'affaires perdu, les références
  orphelines restent visibles avec des colonnes nulles). Les tables users, products et merchants sont diffusées
  (`broadcast`) quand `enable-broadcast = true`.
* **Tranche d'âge** : Jeune < 25, Adulte de 25 à 44, Âge Moyen de 45 à 64, Senior ≥ 65 (le sujet ne classe pas
  l'âge 25 : rattaché à « Adulte »). Âge inconnu : null.
* **is_working_hours** : 1 si 9 ≤ heure ≤ 17 (bornes incluses, configurables).
* **montant_cumule_7j** : somme sur les 7 × 24 h précédentes (transaction courante incluse).
* **is_active_user** : au moins 5 jours calendaires distincts avec transaction sur les 7 derniers jours calendaires.
* **KPI marchands** : ne couvre que les marchands valides ayant au moins une transaction valide ; le « pivot » sur
  `age_group` est écrit avec des sommes conditionnelles (équivalent, 0 si aucune vente).
* **Cohortes** : `cohort_month` = mois de la première transaction ; meilleure cohorte à 3 mois = plus fort taux à
  `period_index = 3` (départage : plus grande cohorte).
* **Orphelins (Q2.5)** : comptés sur les transactions lues, par rapport aux référentiels lus (avant validation).

### Règles de segmentation RFM (Q4.3)

Scores 1 à 5 par `ntile(5)` ; pour la récence, la plus faible obtient 5.

| Segment | Règle | Justification |
|---|---|---|
| Perdus | R ≤ 2 et F ≤ 2 | ancien et peu actif |
| À risque | R ≤ 2 et F ≥ 3 | bon client qui n'a plus acheté récemment |
| Nouveaux | R ≥ 3 et F ≤ 2 | récent, peu de commandes |
| Champions | R ≥ 4, F ≥ 4 et M ≥ 4 | récent, fréquent, dépensier |
| Clients fidèles | tous les autres (R ≥ 3, F ≥ 3) | actifs et réguliers |

Les utilisateurs absents de la table users validée ont le `customer_segment` « Inconnu ».

## 9. Tests

`sbt test` lance 16 tests ScalaTest : fonction derrière l'UDF (bornes, entrées invalides), valeurs par défaut de
la configuration, règles de validation (raisons cumulées, taux de rejet, nulls, orphelins), jointures, fenêtres,
tranches d'âge et détection des transactions suspectes.
