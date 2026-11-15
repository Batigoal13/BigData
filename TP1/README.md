# TP1 — Comptage de mots : quatre méthodes

**Auteur** : Batist Aureille — identifiant ID18 · **Date** : 15 novembre 2026
**Notebook** : [wordcount_mapreduce.ipynb](wordcount_mapreduce.ipynb)

## Objectif

Compter le nombre d'occurrences de chaque mot d'un fichier texte (le *WordCount*, le « Hello World » du Big Data) avec **quatre méthodes** : Python séquentiel, Python pseudo-MapReduce, Hadoop MapReduce et PySpark. On les compare selon trois axes :

| Axe | Question posée |
|---|---|
| **Structure** | Comment le traitement est-il découpé ? |
| **Outils** | Quelles technologies sont mobilisées ? |
| **Temps d'exécution** | Comment la durée évolue-t-elle quand le volume augmente ? |

Les codes des trois premières méthodes sont repris du rapport `ComptageMots.pdf`. PySpark est écrit de deux façons (API RDD et API DataFrame) : les tableaux de résultats ont donc **cinq colonnes de temps**, numérotées de 1 à 5.

## Contenu du dossier

| Fichier | Rôle |
|---|---|
| `wordcount_mapreduce.ipynb` | Le notebook du TP : les quatre méthodes, les mesures et l'analyse |
| `wordcount.py` | Méthode 1 : Python séquentiel (un dictionnaire) |
| `mapreduce_wordcount.py` | Méthode 2 : pseudo-MapReduce en Python (`mapper` → `shuffle` → `reducer`) |
| `ComptageMots.pdf` | Le rapport d'origine dont sont repris les codes |
| `data/mots_N.txt` … `data/mots_10000N.txt` | Les 5 fichiers de test, de 1 000 à 10 millions de lignes (un mot par ligne) |
| `requirements.txt` | Dépendances Python |

Les fichiers `mots_1000N.txt` et `mots_10000N.txt` (61 Mo) sont créés par le notebook, en recopiant `mots_N.txt` 1 000 et 10 000 fois.

## Les quatre méthodes

| # | Méthode | Principe |
|---|---|---|
| 1 | **Python séquentiel** (« Python simple » dans le notebook) | Une boucle et un dictionnaire `mot → nombre`, sur une seule machine et un seul cœur |
| 2 | **Pseudo-MapReduce** | Les étapes de Hadoop reproduites à la main : `mapper` (paires `(mot, 1)`), `shuffle` (regroupement par mot), `reducer` (somme) |
| 3 | **Hadoop MapReduce** | Le job `wordcount` officiel (Java) sur un Hadoop 3.4.1 installé en local, en mode pseudo-distribué (HDFS + YARN) |
| 4 | **PySpark**, API RDD | `textFile → flatMap → map → reduceByKey` |
| 5 | **PySpark**, API DataFrame | `explode(split(...))` puis `groupBy("mot").count()`, l'équivalent d'un `GROUP BY` SQL |

## Installation (macOS)

```bash
python3 -m venv .venv && source .venv/bin/activate      # à la racine du dépôt
pip install -r TP1/requirements.txt
```

- **Java 11 et Hadoop 3.4.1** sont téléchargés automatiquement par le notebook (section 3.1) dans `~/hadoop_tp`. Ce dossier est volontairement sans espace, car les scripts Hadoop gèrent mal les chemins avec espaces. Hadoop ne supporte pas les versions récentes de Java.
- Le notebook démarre directement les 4 démons (NameNode, DataNode, ResourceManager, NodeManager) : `start-dfs.sh` passerait par SSH, désactivé par défaut sur Mac. Il les arrête à la fin.
- **PySpark 3.5** réutilise le Java 11 de `~/hadoop_tp`. En mode local, Spark n'a besoin ni de HDFS ni de YARN.

Le notebook s'exécute de haut en bas, depuis le dossier `TP1/`.

## Plan du notebook

La première cellule donne le titre du TP, la date, l'auteur et l'environnement.

| Sections | Contenu |
|---|---|
| 0 | Les données : cinq fichiers d'un mot par ligne, de 1 000 à 10 millions de lignes |
| 1 | Méthode 1 : Python séquentiel |
| 2 | Méthode 2 : pseudo-MapReduce en Python |
| 3 | Méthode 3 : Hadoop MapReduce (installation, configuration, démarrage de HDFS et YARN, exécution du job) |
| 4 | Comparaison des trois premières méthodes |
| 5 à 8 | Méthode 4 : PySpark (présentation, mise en place, code RDD puis DataFrame, tests) |
| 9 | Mesures : temps, parallélisme, cache en mémoire |
| 10 et 11 | Comparaison des quatre méthodes et conclusion |

## Résultats

Mesures sur un Mac à 10 cœurs. Elles varient légèrement d'une exécution à l'autre.

| Fichier | Lignes | 1. Python simple | 2. Pseudo-MapReduce | 3. Hadoop MapReduce | 4. PySpark RDD | 5. PySpark DataFrame |
|---|---|---|---|---|---|---|
| `mots_N.txt` | 1 000 | 0,0001 s | 0,0004 s | 14,1 s | 0,096 s | 0,054 s |
| `mots_10N.txt` | 10 000 | 0,0011 s | 0,0106 s | 13,1 s | 0,102 s | 0,049 s |
| `mots_100N.txt` | 100 000 | 0,0097 s | 0,0219 s | 13,2 s | 0,107 s | 0,063 s |
| `mots_1000N.txt` | 1 000 000 | 0,0690 s | 0,1837 s | 14,2 s | 0,193 s | 0,172 s |
| `mots_10000N.txt` | 10 000 000 | 0,664 s | 1,846 s | 15,3 s | 1,053 s | **0,410 s** |

Les quatre méthodes (cinq mesures) donnent **exactement les mêmes comptes** (exemple : `bouteille` = 12, 120, 1 200, 12 000, 120 000).

## Ce qu'il faut retenir

1. **Python simple est le plus rapide sur les petits fichiers.** Son temps est proportionnel au volume, mais il est limité à une machine et un cœur.
2. **Le pseudo-MapReduce est 2 à 3 fois plus lent** que Python simple, pour le même calcul : il garde en mémoire une paire `(mot, 1)` par mot (10 millions sur le plus gros fichier). C'est le coût du modèle MapReduce, rentable seulement quand on répartit le travail sur plusieurs machines.
3. **Hadoop met 13 à 15 s, quelle que soit la taille.** Ce temps sert presque entièrement à démarrer Java, YARN et HDFS. Même à 61 Mo, le fichier tient dans un seul bloc HDFS (128 Mo) : un seul *mapper* est lancé, il n'y a donc aucun parallélisme. Le *Combiner* du job officiel résume les 10 millions de paires en quelques totaux avant le *shuffle*.
4. **Spark est 37 fois plus rapide que Hadoop** sur 10 millions de lignes (version DataFrame) : la session est démarrée une seule fois (≈ 1,5 s) et les données intermédiaires restent en mémoire au lieu d'être écrites sur disque.
5. **Sur le plus gros fichier, Spark DataFrame bat même Python simple** (0,41 s contre 0,66 s), grâce aux 10 cœurs.
6. **DataFrame est 2,5 fois plus rapide que RDD** : avec les RDD, chaque mot passe par une fonction Python ; avec les DataFrame, tout reste dans la JVM, optimisé par Catalyst.
7. **Parallélisme** : passer de `local[1]` (4,5 s) à `local[*]` (1,1 s) divise le temps par 4,2, en changeant un seul paramètre.
8. **Cache** : relire `mots_10000N.txt` depuis la mémoire (0,13 s) plutôt que depuis le disque (0,86 s) est près de 7 fois plus rapide.

| | Python simple | Hadoop MapReduce | PySpark |
|---|---|---|---|
| **Code** | 10 lignes | Mapper + reducer + configuration | 5 lignes (RDD) ou une requête type SQL |
| **Données intermédiaires** | En mémoire, une machine | Écrites sur disque entre Map et Reduce | En mémoire, réparties entre cœurs ou machines |
| **Coût de démarrage** | Aucun | ~13 s **à chaque job** | ~1,5 s **une fois** par session |
| **Passage à un cluster** | Impossible | Oui | Oui, avec le même code |

**Conclusion** : pour de petits fichiers sur une machine, Python simple suffit. Spark devient intéressant dès que les données sont volumineuses, que les calculs s'enchaînent ou qu'il faut passer sur un cluster. C'est pourquoi il a largement remplacé Hadoop MapReduce.
