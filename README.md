# Big Data — Rendu des TP

**Auteur** : Batist Aureille — identifiant ID18
**Dépôt** : <https://github.com/Batigoal13/BigData>

Ce dépôt contient les trois TP du cours de Big Data. Ils abordent Hadoop et Spark par des cas pratiques : un comptage de mots, une étude des accidents de la route, puis un projet data complet mené sur petites données et en Big Data.

## 1. Où trouver chaque TP

| TP | Sujet | Dossier | Notebook à ouvrir | Explications |
|---|---|---|---|---|
| **TP1** | Comptage de mots — quatre méthodes : Python séquentiel, PySpark, Python pseudo-MapReduce, Hadoop MapReduce | [TP1/](TP1/) | [wordcount_mapreduce.ipynb](TP1/wordcount_mapreduce.ipynb) | [README du TP1](TP1/README.md) |
| **TP2** | Accidents de la route — PySpark | [TP2/](TP2/) | [Etude de cas_Accidents_routiers_Australie.ipynb](TP2/Etude%20de%20cas_Accidents_routiers_Australie.ipynb) | [README du TP2](TP2/README.md) |
| **TP3** | Deux cas personnels : *small data* | [TP3_ID18/Small/](TP3_ID18/Small/) | [TP3_small_FIFA_WC2026_pipeline.ipynb](TP3_ID18/Small/TP3_small_FIFA_WC2026_pipeline.ipynb) | [README du TP3](TP3_ID18/README.md) |
| **TP3** | Deux cas personnels : *big data* | [TP3_ID18/Big/](TP3_ID18/Big/) | [TP3_big_Airline_BigData_Spark.ipynb](TP3_ID18/Big/TP3_big_Airline_BigData_Spark.ipynb) | [README du TP3](TP3_ID18/README.md) |

Les quatre notebooks sont enregistrés avec leurs résultats (tableaux, graphiques, temps mesurés) : ils se lisent directement sur GitHub, sans rien exécuter.

Les notions techniques des trois TP (Hadoop, Spark, data lake, modèles, mesures d'évaluation) sont résumées dans [NOTIONS.md](NOTIONS.md).

## 2. Organisation du dépôt

```
BigData/
├── README.md                  ce fichier
├── NOTIONS.md                 résumé des notions techniques des trois TP
├── TP1/                       comptage de mots
│   ├── README.md
│   ├── wordcount_mapreduce.ipynb
│   ├── wordcount.py, mapreduce_wordcount.py
│   ├── ComptageMots.pdf       rapport dont sont repris les trois premiers codes
│   └── data/                  5 fichiers de test (1 000 à 10 millions de lignes)
├── TP2/                       accidents de la route
│   ├── README.md
│   ├── Etude de cas_Accidents_routiers_Australie.ipynb
│   ├── crash_data/, units_data/      16 fichiers CSV (≈ 80 Mo)
│   ├── modele_gravite_spark/         modèle sauvegardé par le notebook (partie D)
│   └── metadata-for-road-crash.pdf   description des colonnes
└── TP3_ID18/                  deux cas personnels
    ├── README.md
    ├── Small/                 Coupe du monde 2026 (pandas, scikit-learn, complément PySpark)
    └── Big/                   satisfaction des passagers aériens (PySpark)
```

## 3. Comment les consignes de rendu sont respectées

| Consigne | Où la vérifier |
|---|---|
| **Emplacements** : `TP1/`, `TP2/`, `TP3_ID18/` avec les sous-dossiers `Small/` et `Big/` | Arborescence ci-dessus |
| **Première cellule du TP1** : titre, date, nom, identification | Première cellule de `TP1/wordcount_mapreduce.ipynb` |
| **Première cellule des TP2 et TP3** : titre, puis contribution (le notebook est-il correct ? problèmes rencontrés, modifications apportées, autres informations) | Première cellule des trois notebooks, rubrique « Ma contribution » |
| **Étiquetage du processus IA (TP2 et TP3)** : une cellule `## [Étape]` avant chaque section | Cellules `## [Data]`, `## [EDA]`, `## [Prétraitement]`, `## [Transformation]`, `## [ML]`, `## [Éval]`, `## [Déploiement]` (voir le tableau ci-dessous) |
| **TP3 : traitement avec PySpark** si le notebook d'origine n'en utilise pas | `Big/` : notebook entièrement refait avec PySpark. `Small/` : section 10 ajoutée, « Complément PySpark » |
| **Commentaires, explications et conclusions** | Paragraphes « Lecture » après chaque résultat et conclusion en fin de chaque notebook |
| **README** | Ce fichier, plus un README détaillé dans chaque dossier de TP |

### Étapes du processus IA présentes dans chaque notebook

| Étape | TP2 | TP3 Small | TP3 Big |
|---|:-:|:-:|:-:|
| `[Data]` | ✓ | ✓ | ✓ |
| `[EDA]` | ✓ | ✓ | ✓ |
| `[Prétraitement]` | ✓ | ✓ | ✓ |
| `[Transformation]` | ✓ | ✓ | ✓ |
| `[ML]` | ✓ | ✓ | ✓ |
| `[Éval]` | ✓ | ✓ | ✓ |
| `[Déploiement]` | ✓ | ✓ | ✓ |
| `[Environnement]` (ajoutée) | ✓ | ✓ | ✓ |
| `[Benchmark]` (ajoutée) | ✓ | — | ✓ |
| `[Conclusion]` (ajoutée) | ✓ | ✓ | ✓ |

Les sept étapes de la consigne sont présentes dans les trois notebooks. Dans le TP2, les étapes ML, Éval et Déploiement sont dans la partie D (prédiction de la gravité d'un accident). Les trois étiquettes « ajoutées » complètent la liste de la consigne : démarrage de Spark, mesures de performance et conclusion.

## 4. Résultats principaux

| TP | À retenir |
|---|---|
| **TP1** | Les quatre méthodes donnent les mêmes comptes. Sur 10 millions de lignes : Python séquentiel 0,66 s, pseudo-MapReduce 1,85 s, Hadoop 15,3 s, PySpark 0,41 s (DataFrame). Hadoop paie environ 13 s de démarrage à chaque job ; Spark démarre une fois et calcule en mémoire. |
| **TP2** | Six erreurs du code d'origine corrigées, dont un découpage CSV qui abîmait 16,7 % des lignes. DataFrame et Spark SQL ont les mêmes performances ; le RDD est 3 fois plus lent sur une jointure. La gravité des accidents dépend surtout de la vitesse, de la nuit, de l'alcool et des drogues. Un modèle Spark MLlib prédit les accidents graves de 2018-2019 avec une AUC de 0,84. |
| **TP3 Small** | Prédictions de la Coupe du monde 2026 confrontées aux 104 vrais résultats : RPS de 0,1505 pour notre modèle, contre 0,1535 pour le notebook d'origine. Le traitement refait avec PySpark donne exactement les résultats de pandas, mais il est plus lent à ce volume. |
| **TP3 Big** | 6,5 millions de questionnaires traités avec Spark : data lake Parquet, pipeline Spark ML, forêt aléatoire à 0,987 d'AUC. À ce volume, Spark est 4 à 12 fois plus rapide que pandas. |

## 5. Réexécuter les notebooks

Environnement utilisé : macOS (10 cœurs, 16 Go), Python 3.12, Java 11, Hadoop 3.4.1, Spark 3.5 en mode local.

```bash
# à la racine du dépôt
python3 -m venv .venv && source .venv/bin/activate
pip install -r TP1/requirements.txt -r TP3_ID18/Small/requirements.txt -r TP3_ID18/Big/requirements.txt
```

1. **Commencer par le TP1** : son notebook télécharge Java 11 et Hadoop 3.4.1 dans `~/hadoop_tp`. Les autres notebooks réutilisent ce Java 11 pour Spark.
2. **Ouvrir chaque notebook depuis son propre dossier** (`TP1/`, `TP2/`, `TP3_ID18/Small/`, `TP3_ID18/Big/`) : les chemins vers les données sont relatifs.
3. **Exécuter toutes les cellules de haut en bas.**

Le notebook `Big/` génère environ 1,5 Go de données dans `TP3_ID18/Big/bigdata/` (dossier non versionné) et dure environ 15 minutes au premier lancement.
