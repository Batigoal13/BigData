# TP2 — Accidents de la route en Australie-Méridionale avec PySpark

**Auteur** : Batist Aureille — identifiant ID18
**Notebook** : [Etude de cas_Accidents_routiers_Australie.ipynb](Etude%20de%20cas_Accidents_routiers_Australie.ipynb)

## Objectif

Reproduire, corriger puis compléter les analyses du dépôt
[RakeshNain / Big-data-Analysis-of-Road-Crash-Data-using-PySpark](https://github.com/RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial),
en utilisant les **trois API de Spark** (RDD, DataFrame, Spark SQL) et en mesurant leurs performances. Une dernière partie entraîne un **modèle Spark MLlib** qui prédit la gravité d'un accident.

## Contenu du dossier

| Fichier | Rôle |
|---|---|
| `Etude de cas_Accidents_routiers_Australie.ipynb` | Le notebook du TP |
| `crash_data/` | 8 CSV, un par année (2012-2019) : **une ligne par accident** (lieu, date, conditions, gravité, alcool/drogues…), 127 672 lignes |
| `units_data/` | 8 CSV : **une ligne par « unité » impliquée** (véhicule, piéton, cycliste, obstacle…), 272 591 lignes |
| `metadata-for-road-crash.pdf` | Description des colonnes (source : DPTI, data.sa.gov.au) |
| `modele_gravite_spark/` | Le modèle de la partie D, sauvegardé par le notebook (`PipelineModel` Spark : préparation + gradient boosting) |

Les deux jeux (16 fichiers, environ 80 Mo) sont reliés par la clé `REPORT_ID`.

## Installation

Même environnement que le TP1 : le `.venv` à la racine du dépôt (`pip install -r TP1/requirements.txt`) et Java 11 dans `~/hadoop_tp`, installé par le notebook du TP1. Spark tourne en mode `local[*]` (10 cœurs) avec 4 Go de mémoire pour le *driver*. Le notebook s'exécute depuis le dossier `TP2/`.

## Plan du notebook

La **première cellule** donne le titre du TP puis la contribution : le notebook est-il correct, les problèmes rencontrés, les modifications apportées et les informations utiles.

| Partie | Contenu |
|---|---|
| **A. Reproduction** | Les analyses du dépôt d'origine avec les RDD, les DataFrame et Spark SQL. Les erreurs repérées sont signalées par ⚠️ et corrigées |
| **B. Benchmark** | Comparaison RDD / DataFrame / SQL : temps d'exécution, plans d'exécution, nombre de cœurs, partitions de *shuffle* |
| **C. Nouvelles analyses** | Quand, où et dans quelles conditions les accidents sont-ils les plus graves ? |
| **D. Machine learning** | Prédire la gravité d'un accident avec Spark MLlib : table d'apprentissage, pipeline, trois modèles, évaluation, sauvegarde du modèle |

## Étapes du processus IA

Chaque section du notebook est précédée d'une cellule d'étiquette `## [Étape]`.

| Étiquette | Sections du notebook | Contenu |
|---|---|---|
| `[Data]` | 0.1, début de A.1 | Contrôle des 16 fichiers CSV, chargement dans des RDD |
| `[Environnement]` | 0.2 | Démarrage de Spark |
| `[Prétraitement]` | A.1, A.2 | Découpage correct des lignes CSV, lecture en DataFrame, typage des colonnes |
| `[Transformation]` | A.1, début de la partie C, D.1 | Partitionnement personnalisé ; colonnes dérivées (`Heure`, `Grave`, `Mortel`) ; table d'apprentissage et pipeline de préparation |
| `[EDA]` | A.1 à A.4, C.1 à C.5 | Toutes les analyses : requêtes du dépôt d'origine, puis nouvelles analyses |
| `[ML]` | D.2 | Entraînement de trois modèles : régression logistique, forêt aléatoire, gradient boosting |
| `[Éval]` | D.3 | AUC, rappel, précision, courbe ROC, choix du seuil, effet propre de chaque facteur |
| `[Déploiement]` | D.4 | Sauvegarde du modèle, rechargement et prédiction sur de nouveaux accidents |
| `[Benchmark]` | Partie B | Comparaison des performances RDD / DataFrame / SQL |
| `[Conclusion]` | Conclusion | Synthèse, limites et pistes |

Les sept étapes de la consigne sont présentes. Les étiquettes `[Environnement]`, `[Benchmark]` et `[Conclusion]` ont été ajoutées à la liste de la consigne.

## Corrections apportées au code d'origine

| Problème | Conséquence | Correction |
|---|---|---|
| Découpage des lignes avec `split(',')` | 16,7 % des lignes d'accident sont mal découpées : un champ entre guillemets contient une virgule (`"CC OF NORWOOD,PAYNEHAM & ST PETERS"`) et décale toutes les colonnes | Module `csv` appliqué avec `mapPartitions` ; lecteur CSV de Spark avec `escape='"'` |
| Le partitionnement dit utiliser `Lic State` mais lit `Veh Reg State` | Analyse mal nommée | Colonne réellement utilisée, correctement nommée |
| `groupByKey` pour une moyenne | Toutes les valeurs traversent le *shuffle* | `reduceByKey` avec des couples `(somme, nombre)`, l'équivalent du *Combiner* de Hadoop |
| `sortByKey().collect()` pour garder 5 éléments | Tri et rapatriement de tout le RDD | `takeOrdered(5)` |
| 4 fonctions Python (UDF) et des variables globales pour des pourcentages | Allers-retours JVM ↔ Python | Une fonction de fenêtre (`Window`) |
| Requête « victimes par quartier, conducteur sans permis » | **Trois résultats différents** selon l'API : filtre inversé, comptage au lieu de somme, découpage décalé | Même définition dans les 3 API, résultats vérifiés identiques |

## Résultats du benchmark (partie B)

Médiane de 3 essais sur un Mac à 10 cœurs :

- **Jointure + agrégation (Q3)** : RDD 0,84 s, DataFrame 0,27 s, SQL 0,28 s. Le RDD est **3,1 fois plus lent**.
- **DataFrame = SQL** : même optimiseur (Catalyst), même plan physique. Dans ce plan, Spark ne lit que les colonnes utiles (*column pruning*), applique le filtre dès la lecture (*predicate pushdown*) et copie la petite table sur chaque executor (*broadcast join*). Le RDD, lui, exécute le code tel quel et fait passer chaque ligne par Python.
- **Nombre de cœurs** : de 1 à 10 cœurs, le RDD est accéléré 3 fois, les DataFrame/SQL 1,3 à 1,6 fois. L'accélération est sous-linéaire sur 80 Mo, car les coûts fixes (lecture, planification, *shuffle*) dominent.
- **Partitions de *shuffle*** : Q3 prend 0,19 s avec 4 à 8 partitions, contre 1,5 s avec 1 000. L'*Adaptive Query Execution* (AQE) règle ce problème automatiquement.

| | RDD | DataFrame | Spark SQL |
|---|---|---|---|
| **Abstraction** | Bas niveau : on décrit *comment* calculer | Haut niveau : on décrit *quoi* calculer | Requête SQL déclarative |
| **Optimisation** | Aucune | Catalyst + Tungsten | Identique au DataFrame |
| **Quand l'utiliser** | Données non structurées, logique impossible en colonnes | Cas général en Python | Requêtes complexes lisibles |

## Résultats de l'analyse (partie C)

L'indicateur principal est le **taux d'accidents graves** (au moins un blessé grave ou un mort), car un simple nombre d'accidents reflète surtout le trafic.

- **Quand ?** Le nombre d'accidents suit le trafic (heures de pointe), mais leur gravité est maximale **la nuit** : 5,6 à 9,5 % d'accidents graves entre 19 h et 4 h, contre 2,8 % à 8 h. Elle l'est aussi le **week-end** (6,3 % le dimanche, contre environ 4 % en semaine).
- **Évolution 2012-2019** : les accidents enregistrés baissent de 43 %, mais les décès restent stables (80 à 114 par an). La chute brutale de 2012 à 2013 suggère un changement des règles de déclaration.
- **Conditions** : la **vitesse** est le facteur le plus marquant, avec environ 5 fois plus d'accidents graves à 100-110 km/h qu'en ville. Les zones rurales, les routes non goudronnées, les virages et la nuit aggravent aussi. Paradoxe : la pluie ne rend pas les accidents plus graves.
- **Qui ?** Les conducteurs **suspendus ou sans permis** (9,8 à 11,9 % d'accidents graves contre 3,4 % avec un permis complet), les **plus de 75 ans** et les usagers vulnérables (motos, piétons, cyclistes).
- **Alcool et drogues** : la part d'accidents mortels passe de 0,4 % (aucune substance) à 13 % (alcool + drogues), soit **plus de 30 fois** plus. Il faut garder en tête un biais de dépistage, plus systématique après un accident grave.
- **Où ?** Les accidents se concentrent autour d'Adélaïde, mais les accidents mortels sont dispersés sur le réseau rural.

## Résultats du modèle (partie D)

**But** : prédire si un accident est grave (au moins un blessé grave ou un mort) à partir de ses circonstances, et mesurer l'effet propre de chaque facteur.

**Méthode** : une ligne par accident (les unités impliquées sont agrégées puis jointes), 28 variables, classes pondérées car seuls 4 à 5 % des accidents sont graves. Le découpage suit le temps : apprentissage sur 2012-2016, choix du modèle sur 2017, test sur 2018-2019. Les colonnes qui comptent les victimes sont exclues, car elles contiennent la réponse.

| Modèle | AUC ROC (test) | AUC PR (test) | Rappel | Précision |
|---|---|---|---|---|
| Hasard / toujours « non grave » | 0,500 | 0,053 | 0 % | — |
| Régression logistique | 0,842 | 0,277 | 78 % | 15 % |
| Forêt aléatoire | 0,827 | 0,222 | 75 % | 16 % |
| **Gradient boosting** (retenu sur la validation) | **0,844** | 0,267 | 79 % | 15 % |

- Le modèle détecte 79 % des accidents graves, mais avec beaucoup de fausses alertes : la gravité dépend aussi de facteurs absents des données.
- L'accuracy est trompeuse ici : répondre toujours « non grave » donne 95 % de bonnes réponses sans rien détecter.
- **Effet propre des facteurs** (rapports de cotes de la régression logistique, à autres facteurs égaux) : deux-roues motorisé × 8, piéton ou cycliste × 6, drogues × 6,7, alcool × 2,5, zone rurale × 2,3, + 10 km/h de vitesse limite × 1,3.
- La route non goudronnée (× 3,9 en comparaison simple, × 1,2 dans le modèle) et le virage (× 3,0, puis × 1,25) s'expliquent surtout par les autres facteurs. La pluie reste non aggravante (× 0,8).

## Ce qu'il faut retenir

- Une fois les erreurs corrigées, les trois API donnent **les mêmes résultats**. DataFrame et SQL sont aussi rapides l'un que l'autre ; le RDD est nettement plus lent, surtout sur les jointures.
- **Bonne pratique** : agréger dans Spark, puis ne convertir en pandas que les petits résultats à tracer.
- Les comparaisons simples de la partie C mélangent des facteurs liés (une route rurale est souvent rapide, sombre et sinueuse). Le modèle de la partie D **isole l'effet de chacun**. Cela reste des associations, pas des preuves de causalité.
