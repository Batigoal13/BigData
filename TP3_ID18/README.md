# TP3 (ID18) — Deux cas personnels : small data et big data

**Auteur** : Batist Aureille — identifiant ID18

| Cas | Dossier | Notebook |
|---|---|---|
| *Small data* | [Small/](Small/) | [TP3_small_FIFA_WC2026_pipeline.ipynb](Small/TP3_small_FIFA_WC2026_pipeline.ipynb) |
| *Big data* | [Big/](Big/) | [TP3_big_Airline_BigData_Spark.ipynb](Big/TP3_big_Airline_BigData_Spark.ipynb) |

## Objectif

Dérouler **toutes les étapes** d'un projet de data science, de la donnée brute jusqu'à un modèle prêt à déployer, dans deux situations :

| | `Small/` — petites données | `Big/` — Big Data |
|---|---|---|
| **Sujet** | Prédire les matchs de la Coupe du monde 2026 | Prédire la satisfaction des passagers aériens |
| **Notebook de départ** | [FIFA WC 2026 Predictions](https://www.kaggle.com/code/mdbayzidibrahim/fifa-wc-2026-predictions) | [Airline Satisfaction — EDA & ML 96 %](https://www.kaggle.com/code/nadenkhaled/airline-satisfaction-eda-ml-96) |
| **Volume** | ≈ 50 000 matchs, quelques Mo | 6,5 millions de questionnaires, 900 Mo de CSV |
| **Notebook d'origine avec PySpark ?** | Non | Non |
| **Outils** | pandas, scikit-learn, XGBoost ; **complément PySpark** (section 10) | **PySpark** du début à la fin (DataFrame, SQL, Spark ML / MLlib) |
| **Étapes** | Data → EDA → Prétraitement → Transformation → ML → Éval → Déploiement, puis le traitement des données refait avec PySpark | Data → EDA → Prétraitement → Transformation → ML → Éval → Déploiement, entièrement avec Spark |

Les deux sujets sont **indépendants**. On compare surtout la **démarche** : ce qui change quand le volume impose de passer à Spark, et ce qui ne change pas.

**Première cellule de chaque notebook** : le titre, puis la contribution (le notebook est-il correct ? problèmes rencontrés, modifications apportées, autres informations).

**Étiquetage du processus IA** : chaque section est précédée d'une cellule `## [Étape]` : `[Data]`, `[EDA]`, `[Prétraitement]`, `[Transformation]`, `[ML]`, `[Éval]`, `[Déploiement]`. Trois étiquettes ont été ajoutées à la liste de la consigne : `[Environnement]` (mise en place), `[Benchmark]` (mesures de performance, dans `Big/`) et `[Conclusion]`.

Dans chaque notebook, les erreurs repérées dans le notebook Kaggle d'origine sont signalées par ⚠️, corrigées et récapitulées dans la section *Bilan*. Les modèles finaux sont sauvegardés dans `artefacts/` pour l'étape suivante, le **déploiement** (une API dans un conteneur Docker), qui sera ajoutée plus tard.

## Organisation

```
TP3_ID18/
├── Small/
│   ├── TP3_small_FIFA_WC2026_pipeline.ipynb
│   ├── result_matchs_dataset/    résultats 1872-2026, tirs au but, buteurs, anciens noms,
│   │                             notes EA FC 26, joueurs et sélections Transfermarkt
│   ├── artefacts/                modèle de buts (joblib), état des équipes, configuration, probabilités
│   └── requirements.txt
└── Big/
    ├── TP3_big_Airline_BigData_Spark.ipynb
    ├── airline_dataset/          train.csv et test.csv (129 880 questionnaires)
    ├── bigdata/                  data lake généré par le notebook (≈ 1,5 Go, non versionné)
    ├── artefacts/                PipelineModel Spark (préparation + forêt aléatoire)
    └── requirements.txt
```

## `Small/` — Coupe du monde 2026 (pandas / scikit-learn, complément PySpark)

Le notebook Kaggle a été écrit **avant** le tournoi. Nos données contiennent les **104 vrais résultats** : on peut donc confronter ses prédictions à la réalité.

| Étape | Contenu |
|---|---|
| **Data** | 7 fichiers ; données gelées au 11 juin 2026 (coup d'envoi) ; les 104 matchs du tournoi sont mis de côté jusqu'à l'évaluation finale |
| **EDA** | Avantage du terrain, part de nuls (≈ 1 match sur 4), loi du nombre de buts (Poisson), qualité des données joueurs |
| **Prétraitement** | Tri chronologique stable, harmonisation des noms de pays entre sources, filtrage des joueuses, vérifications |
| **Transformation** | Classement **Elo** calculé match après match, variables d'avant-match (forme, attaque, défense), « facteurs » d'effectif (notes FC 26, ligues, expérience) |
| **ML** | Reproduction des modèles du notebook, puis amélioration : 9 variables, boosting réglé par validation croisée temporelle, **régression de Poisson + Dixon-Coles** |
| **Éval** | Test 2021-2026, calibration, puis **confrontation aux 104 matchs réels** (RPS, log-loss, bootstrap) et à 10 000 simulations du tournoi |
| **Déploiement** | Sauvegarde des artefacts nécessaires à une future API |
| **Complément PySpark** (section 10) | Ingestion avec schéma explicite, contrôle qualité, EDA (API DataFrame et Spark SQL), variables d'avant-match par fonctions de fenêtre ; chaque résultat est vérifié contre pandas |

**Résultats**
- Sur la vraie Coupe du monde, notre modèle final (Poisson + Dixon-Coles + facteurs) obtient un RPS de **0,1505**, contre 0,1535 pour le notebook d'origine et 0,229 pour une prédiction naïve. L'écart avec le notebook n'est pas significatif sur un seul tournoi.
- Le notebook d'origine avait bien vu l'Espagne favorite et la finale Espagne – Argentine. Les 4 demi-finalistes réels sont les 4 premiers du classement Elo au coup d'envoi.
- Complexifier le ML n'apporte presque rien : l'Elo résume déjà l'information des résultats passés. Ce qui aide, c'est une **information nouvelle** (la qualité des effectifs) et un modèle **cohérent** pour les probabilités, les scores et la simulation.
- **Avec PySpark** : le traitement des données redonne exactement les résultats de pandas (mêmes volumes, mêmes statistiques, même état des 337 équipes). Sur 49 475 matchs, Spark est 2,5 à 17 fois plus lent que pandas : ses coûts fixes ne sont rentables qu'à grand volume. Le classement Elo, séquentiel par nature, reste en Python.
- 9 corrections du code d'origine, dont : un paramètre scikit-learn supprimé qui faisait planter le code, deux modèles contradictoires, une loi de Poisson biaisée, des tirs au but supposés gagnés par le favori (le favori n'en gagne que 53 %).

## `Big/` — Satisfaction des passagers (Spark)

On imagine que la compagnie collecte ses questionnaires sur trois ans. Les 130 000 réponses d'origine sont dupliquées **50 fois en les modifiant légèrement** (âge, distance, notes, retards), avec des défauts réalistes injectés (doublons, âges aberrants, formats incohérents) : on obtient un univers de **6,5 millions de lignes**.

| Étape | Contenu |
|---|---|
| **Data** | Génération distribuée de l'univers ; ingestion CSV avec schéma explicite ; data lake **brut → bronze → silver → gold** en Parquet |
| **EDA** | Agrégations distribuées (seuls les résultats agrégés vont dans pandas) ; benchmark pandas / Spark CSV / Spark Parquet sur ×1, ×10, ×50 |
| **Prétraitement** | Doublons, valeurs aberrantes, formats ; stockage **partitionné par mois** |
| **Transformation** | `Pipeline` Spark ML : `Imputer`, `SQLTransformer`, `StringIndexer`, `VectorAssembler`, `StandardScaler` |
| **ML** | Spark MLlib : régression logistique, arbre de décision, forêt aléatoire, gradient boosting |
| **Éval** | AUC, précision, rappel, F1 ; comparaison avec le notebook Kaggle ; piège des quasi-doublons ; courbe d'apprentissage |
| **Déploiement** | Sauvegarde du `PipelineModel` complet (préparation + modèle) |
| **Benchmark** | Effet du nombre de cœurs sur le temps de traitement |

**Résultats**
- À 6,5 millions de lignes, la même agrégation prend 5,9 s en pandas, 1,3 s en Spark (CSV) et 0,5 s en Spark (Parquet). Parquet est 6,5 fois plus compact que le CSV.
- Meilleur modèle : **forêt aléatoire, AUC 0,987**. Les variables clés sont l'embarquement en ligne, le wifi, le motif du voyage et la classe.
- **Le volume ne remplace pas l'information** : XGBoost entraîné sur les 104 000 lignes d'origine reste meilleur (96,3 % contre 94,6 %), et 1 % des données suffit presque à atteindre le score maximal.
- **Piège des quasi-doublons** : découper apprentissage et test au hasard surestime le score de près de 4 points. Il faut découper **par passager**.
- Contrainte mémoire : avec 100 copies (13 millions de lignes), l'entraînement saturait la mémoire Java d'un Mac de 16 Go, d'où le choix de 50 copies et de données relues depuis le disque (zone gold).

## Installation

Depuis la racine du dépôt, avec le `.venv` des TP précédents :

```bash
pip install -r TP3_ID18/Small/requirements.txt -r TP3_ID18/Big/requirements.txt
```

- `Big/` et la section 10 de `Small/` utilisent le Java 11 installé dans `~/hadoop_tp` au TP1. Dans `Big/`, Spark démarre avec 10 Go de mémoire pour le *driver*.
- Chaque notebook s'exécute **depuis son propre dossier** (`Small/` ou `Big/`). Le notebook `Big/` régénère son data lake au premier lancement (environ 15 minutes au total, dont l'entraînement des modèles).
- **macOS (Apple Silicon)** : XGBoost a besoin de la bibliothèque `libomp`. Si elle n'est pas installée par Homebrew, on peut faire pointer XGBoost vers celle fournie avec scikit-learn (commande dans `Small/requirements.txt`).
