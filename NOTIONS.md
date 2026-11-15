# Notions techniques des TP

Ce fichier résume les notions utilisées dans les trois TPs du dépôt : ce que chacune veut dire, à quoi elle sert et où la retrouver. Il complète le [README](README.md), qui explique l'organisation du rendu.

## Comment lire ce fichier

- Chaque notion est expliquée en quelques lignes, avec l'endroit où elle apparaît (« Où »).
- Le signe ➕ marque une **notion complémentaire**, qui va au-delà des consignes des TP.

> **Usage de l'IA** : les notions complémentaires (➕) ont été suggérées par IA, afin d'aboutir à des conclusions plus intéressantes sur le plan pédagogique et analytique.

## Sommaire

1. [Hadoop et MapReduce](#1-hadoop-et-mapreduce)
2. [Spark et PySpark](#2-spark-et-pyspark)
3. [Stockage des données](#3-stockage-des-données)
4. [Préparer un modèle](#4-préparer-un-modèle)
5. [Les modèles utilisés](#5-les-modèles-utilisés)
6. [Évaluer un modèle](#6-évaluer-un-modèle)
7. [Interpréter un résultat](#7-interpréter-un-résultat)

---

## 1. Hadoop et MapReduce

| Notion | En bref | Où |
|---|---|---|
| **MapReduce** | Façon de découper un calcul en trois étapes pour le répartir sur plusieurs machines. *Map* : chaque ligne produit des paires (clé, valeur), par exemple `(mot, 1)`. *Shuffle* : les paires de même clé sont regroupées. *Reduce* : les valeurs de chaque clé sont combinées, par exemple additionnées. | TP1, sections 2 et 3 |
| **Hadoop** | Logiciel qui exécute des calculs MapReduce sur un groupe de machines (un *cluster*). Il réunit un système de fichiers (HDFS) et un gestionnaire de ressources (YARN). | TP1, section 3 |
| **HDFS** | Système de fichiers de Hadoop. Un fichier est coupé en blocs de 128 Mo, répartis sur les machines. Le *NameNode* sait où est chaque bloc, les *DataNodes* les stockent. | TP1, section 3 |
| **YARN** | Répartit le processeur et la mémoire entre les calculs. Le *ResourceManager* décide, les *NodeManagers* lancent les tâches. | TP1, section 3 |
| **Mode pseudo-distribué** | Tous les services Hadoop tournent sur une seule machine, mais communiquent comme sur un vrai cluster. C'est le mode utilisé ici. | TP1, section 3 |
| **Combiner** | Mini-*reduce* exécuté dès la sortie du *map*, avant le *shuffle*. Il résume les paires sur place : moins de données circulent. | TP1, section 3 ; TP2, A.1 |

**À retenir** : Hadoop met 13 à 15 s par calcul quelle que soit la taille du fichier, car il redémarre ses services Java et écrit sur disque entre les étapes. Il n'est rentable que sur de très gros volumes.

---

## 2. Spark et PySpark

### Les bases

| Notion | En bref | Où |
|---|---|---|
| **Spark** | Moteur de calcul distribué, créé pour dépasser les limites de Hadoop MapReduce. Il garde les données intermédiaires en mémoire et ne démarre ses services qu'une fois par session. | TP1, sections 5 à 11 |
| **PySpark** | L'interface Python de Spark : on écrit du Python, Spark exécute le calcul dans la machine Java. | Tous les TP |
| **Driver et executors** | Le *driver* pilote le calcul et distribue le travail ; les *executors* l'exécutent. En mode local (`local[*]`), chaque cœur du processeur joue le rôle d'une machine. | TP1, section 5 |
| **Partition** | Morceau d'un jeu de données. Chaque partition est traitée par un cœur ou une machine : c'est ce qui permet le parallélisme. | TP1, TP2, TP3 Big |
| **Transformation et action** | Une transformation (`map`, `filter`, `groupBy`) décrit un calcul sans l'exécuter. Une action (`count`, `show`, `collect`) demande un résultat et déclenche le calcul. | TP1, section 7 |
| **Évaluation paresseuse** | Conséquence du point précédent : Spark attend une action pour calculer. Il peut ainsi organiser au mieux l'ensemble des opérations. | TP1, section 7 |
| **Shuffle** | Redistribution des données entre partitions, nécessaire pour regrouper par clé ou faire une jointure. C'est l'opération la plus coûteuse. | TP1, TP2 |
| **Cache** | Garde un jeu de données en mémoire pour le réutiliser sans relire le disque. | TP1, section 9.3 |

### Les trois façons d'écrire un calcul

| Notion | En bref | Où |
|---|---|---|
| **RDD** | Collection d'objets répartie en partitions. On décrit *comment* calculer, avec des fonctions Python. Aucune optimisation automatique. | TP1, section 7.1 ; TP2, A.1 |
| **DataFrame** | Table répartie, avec des colonnes nommées et typées. On décrit *ce que l'on veut*, Spark choisit comment le faire. | TP1, section 7.2 ; TP2, A.2 |
| **Spark SQL** | Le même DataFrame, interrogé avec une requête SQL. | TP2, A.4 ; TP3 |
| **Catalyst** | L'optimiseur de Spark pour les DataFrame et le SQL. Il réécrit la requête : filtrer plus tôt, ne lire que les colonnes utiles, choisir le type de jointure. | TP2, B.2 |

**À retenir** : DataFrame et SQL ont les mêmes performances, car ils passent par le même optimiseur. Le RDD est plus lent (3 fois sur une jointure dans le TP2), car chaque ligne passe par Python.

### Pour aller plus loin

| Notion | En bref | Où |
|---|---|---|
| ➕ **Plan d'exécution** | La liste des opérations que Spark va réellement exécuter, affichée par `explain()`. Il permet de vérifier ce que fait l'optimiseur. | TP2, B.2 |
| ➕ **Column pruning et predicate pushdown** | Deux optimisations visibles dans le plan : ne lire que les colonnes utiles, et appliquer les filtres dès la lecture du fichier. | TP2, B.2 |
| ➕ **Broadcast join** | Jointure où la petite table est copiée sur chaque executor, ce qui évite un *shuffle* de la grande table. | TP2, B.2 ; TP3 Small, 10.3 |
| ➕ **AQE** (*Adaptive Query Execution*) | Spark ajuste le plan pendant l'exécution, par exemple en fusionnant les partitions trop petites après un *shuffle*. | TP2, B.4 |
| ➕ **Accélération** (*speedup*) | Temps avec 1 cœur divisé par le temps avec *n* cœurs. Dans l'idéal elle vaut *n* ; en pratique elle est plus faible, à cause des coûts fixes. | TP1, 9.2 ; TP2, B.3 ; TP3 Big, section 8 |
| ➕ **Fonction de fenêtre** (`Window`) | Calcul sur un groupe de lignes voisines sans les regrouper : un pourcentage par catégorie, une moyenne sur les 10 matchs précédents. | TP2, A.3 ; TP3 Small, 10.5 |
| **UDF** | Fonction Python appliquée ligne par ligne dans Spark. Elle est lente, car chaque ligne fait un aller-retour entre Java et Python. À éviter quand une fonction Spark existe. | TP2, A.3 |

---

## 3. Stockage des données

| Notion | En bref | Où |
|---|---|---|
| **CSV** | Fichier texte, une ligne par enregistrement. Simple, mais volumineux et lent à lire : pour une seule colonne, il faut lire tout le fichier. | Tous les TP |
| **Schéma explicite** | On déclare le type de chaque colonne au lieu de laisser Spark le deviner. La lecture est plus rapide et les erreurs de format sont détectées. | TP3 Big, 1.3 ; TP3 Small, 10.2 |
| ➕ **Parquet** | Format de fichier en colonnes, compressé. Spark ne lit que les colonnes demandées. Dans le TP3, il est 6,5 fois plus compact que le CSV et bien plus rapide à interroger. | TP3 Big, 1.4 |
| ➕ **Data lake** | Espace de stockage où l'on conserve les données à plusieurs stades de préparation, des fichiers bruts jusqu'aux tables prêtes à l'emploi. | TP3 Big, section 1 |
| ➕ **Stockage partitionné** | Les fichiers sont rangés dans un dossier par valeur d'une colonne (ici le mois). Une requête sur un mois ne lit que ce dossier. | TP3 Big, section 3 |

### ➕ Le data lake en quatre zones

Chaque zone a un rôle. On ne modifie jamais une zone en place : on écrit la suivante. On peut donc toujours repartir de l'étape précédente.

| Zone | Contenu | Format |
|---|---|---|
| **Brut** | Les données telles qu'elles arrivent, avec leurs défauts | CSV |
| **Bronze** | Les mêmes données, converties dans un format efficace | Parquet |
| **Silver** | Les données nettoyées : doublons, valeurs aberrantes et formats corrigés | Parquet, partitionné par mois |
| **Gold** | Les données prêtes pour le modèle : variables construites, apprentissage et test séparés | Parquet |

---

## 4. Préparer un modèle

| Notion | En bref | Où |
|---|---|---|
| **Étapes du processus IA** | Data → EDA → Prétraitement → Transformation → ML → Éval → Déploiement. Chaque section des notebooks des TP2 et TP3 porte l'étiquette de son étape. | TP2, TP3 |
| **EDA** (analyse exploratoire) | Regarder les données avant de modéliser : volumes, répartitions, valeurs manquantes, liens entre variables. | TP2, TP3 |
| **Pipeline** | Suite d'étapes de préparation suivie d'un modèle, enregistrée comme un seul objet. Les réglages sont appris sur l'apprentissage, puis réappliqués tels quels aux nouvelles données. | TP2, D.1 ; TP3 Big, section 4 |
| **Imputation** | Remplacer une valeur manquante, ici par la médiane calculée sur l'apprentissage. | TP2, D.1 ; TP3 Big, section 4 |
| **Indicatrice** | Colonne 0/1 qui dit si une ligne a une certaine modalité (« zone rurale : oui ou non »). Elle rend une variable en catégories utilisable par un modèle. | TP2, D.1 |
| ➕ **Fuite d'information** | Le modèle utilise une information qu'il n'aurait pas au moment de prédire, ce qui donne des scores trompeurs. Exemples : le nombre de victimes pour prédire la gravité ; des données de joueurs extraites après le tournoi. | TP2, D.1 ; TP3 Small, 1.2 |
| ➕ **Découpage dans le temps** | On apprend sur le passé et on teste sur le futur, au lieu de découper au hasard. C'est la situation réelle d'un modèle. | TP2, D.1 ; TP3 Small, 5.1 |
| ➕ **Validation temporelle** | Même idée pour régler un modèle : chaque essai apprend sur une période et se vérifie sur la période suivante (`TimeSeriesSplit`). Le jeu de test reste intact jusqu'à la fin. | TP3 Small, 5.3 ; TP2, D.3 |
| ➕ **Classes déséquilibrées** | Une des deux réponses est rare (5 % d'accidents graves). Sans précaution, le modèle répond toujours « non ». On donne donc plus de poids aux cas rares pendant l'apprentissage. | TP2, D.1 |
| ➕ **Quasi-doublons** | Lignes presque identiques. Si l'une est dans l'apprentissage et l'autre dans le test, le score est surestimé (près de 4 points dans le TP3). Il faut découper par individu. | TP3 Big, 6.4 |

---

## 5. Les modèles utilisés

| Notion | En bref | Où |
|---|---|---|
| **Régression logistique** | Calcule une probabilité à partir d'une somme pondérée des variables. Simple, rapide, et ses coefficients s'interprètent. | TP2, D.2 ; TP3 |
| **Arbre de décision** | Suite de questions « oui / non » sur les variables. Sans limite de profondeur, il apprend les données par cœur. | TP3 Big, section 5 |
| **Forêt aléatoire** | Moyenne de nombreux arbres, chacun construit sur une partie des données et des variables. Plus stable qu'un seul arbre. | TP2, D.2 ; TP3 |
| **Gradient boosting** | Suite de petits arbres, chacun corrigeant les erreurs des précédents. XGBoost en est une version très utilisée. | TP2, D.2 ; TP3 |
| **Spark MLlib** | La bibliothèque de modèles de Spark : l'entraînement est réparti sur les cœurs ou les machines. | TP2, partie D ; TP3 Big |
| **Classement Elo** | Note par équipe, mise à jour après chaque match selon le résultat et la force de l'adversaire. Battre un adversaire fort rapporte plus. Il vient du notebook d'origine du TP3 small. | TP3 Small, 4.1 |
| **Loi de Poisson** | Loi de probabilité d'un nombre d'événements rares, comme les buts d'un match. On prédit le nombre moyen de buts de chaque équipe, puis on en déduit la probabilité de chaque score. | TP3 Small, 2.3 et 5.4 |
| ➕ **Dixon-Coles** | Correction du modèle de Poisson. Celui-ci suppose que les buts des deux équipes sont indépendants, ce qui sous-estime les petits scores serrés. Un paramètre ρ ajuste la probabilité des scores 0-0, 1-0, 0-1 et 1-1. Ici ρ = −0,1, ce qui remonte un peu les 0-0 et 1-1. | TP3 Small, 5.4 |

**À retenir** : dans les trois TP, passer d'un modèle simple à un modèle complexe apporte peu. Ce qui améliore les résultats, c'est une information nouvelle (la qualité des effectifs dans le TP3 Small).

---

## 6. Évaluer un modèle

### Quand le modèle répond « oui » ou « non »

| Notion | En bref | Où |
|---|---|---|
| **Accuracy** | Part de bonnes réponses. Trompeuse si une réponse est rare : dire toujours « non grave » donne 95 % sans rien détecter. | TP2, D.3 ; TP3 Big |
| **Rappel** | Parmi les vrais « oui », la part que le modèle a trouvée. | TP2, D.3 ; TP3 Big, section 6 |
| **Précision** | Parmi les « oui » annoncés, la part qui est juste. | TP2, D.3 ; TP3 Big, section 6 |
| **F1** | Moyenne (harmonique) du rappel et de la précision. | TP2, D.3 ; TP3 Big, section 6 |
| **Matrice de confusion** | Tableau qui croise la réponse annoncée et la vraie réponse. | TP3 Big, 6.3 |
| ➕ **Seuil de décision** | Score à partir duquel on répond « oui ». Le baisser augmente le rappel et diminue la précision. Le bon seuil dépend de l'usage. | TP2, D.3 |
| ➕ **AUC ROC** | Mesure si le modèle classe bien les « oui » au-dessus des « non », quel que soit le seuil. 0,5 = hasard, 1 = parfait. | TP2, D.3 ; TP3 Big, section 6 |
| ➕ **AUC PR** | Même idée, centrée sur la réponse rare. Le hasard donne la part de « oui » dans les données. | TP2, D.3 |

### Quand le modèle annonce des probabilités

| Notion | En bref | Où |
|---|---|---|
| ➕ **Log-loss** | Pénalise les probabilités éloignées de la réalité, et très fortement une erreur annoncée avec assurance. Plus elle est basse, mieux c'est. | TP3 Small, sections 5 à 7 |
| ➕ **RPS** (*Ranked Probability Score*) | Mesure adaptée aux résultats ordonnés : défaite, nul, victoire. Annoncer « nul » quand il y a victoire est moins grave qu'annoncer « défaite ». 0 = parfait ; plus il est bas, mieux c'est. | TP3 Small, sections 5 à 7 |
| ➕ **Calibration** | Un modèle est calibré si ses probabilités se vérifient : quand il annonce 60 %, l'événement arrive environ 60 % du temps. | TP3 Small, 6.2 |

Le RPS se calcule sur les probabilités cumulées. Pour un match à trois issues :

```
RPS = 1/2 × [ (P(défaite) − O(défaite))²  +  (P(défaite ou nul) − O(défaite ou nul))² ]
```

où `P` est la probabilité annoncée et `O` vaut 1 si l'événement s'est produit, 0 sinon.

### Quand on compare deux modèles

| Notion | En bref | Où |
|---|---|---|
| ➕ **Bootstrap** | Méthode pour mesurer l'incertitude d'un résultat quand on a peu de données. On retire au hasard, avec remise, un échantillon de même taille (ici 104 matchs), 5 000 fois, et on regarde comment le résultat varie. | TP3 Small, 7.3 |
| ➕ **Écart significatif** | Un écart est significatif s'il ne peut pas s'expliquer par le hasard. Dans le TP3 Small, le modèle final est meilleur dans 84 % des tirages bootstrap : c'est encourageant, mais pas une preuve sur un seul tournoi. | TP3 Small, 7.3 |
| ➕ **Courbe d'apprentissage** | Score du modèle selon la quantité de données d'apprentissage. Elle montre si ajouter des données aide encore : dans le TP3 Big, 1 % des données suffit presque. | TP3 Big, 6.5 |
| ➕ **Simulation de Monte-Carlo** | Rejouer un événement un grand nombre de fois avec des tirages au hasard (ici 10 000 tournois) pour estimer des probabilités, comme celle d'être champion. | TP3 Small, 7.5 |

---

## 7. Interpréter un résultat

| Notion | En bref | Où |
|---|---|---|
| **Corrélation et causalité** | Deux choses qui varient ensemble ne sont pas forcément cause l'une de l'autre. Exemple : on dépiste davantage l'alcool après un accident grave, ce qui gonfle le lien apparent. | TP2, A.3 et partie C |
| ➕ **Taux plutôt que nombre** | Un nombre d'accidents reflète surtout le trafic. Pour comparer des situations, on regarde la part d'accidents graves. | TP2, partie C |
| ➕ **Cote** | Autre façon d'écrire une probabilité : `p / (1 − p)`. Une probabilité de 20 % correspond à une cote de 0,25, soit « 1 contre 4 ». | TP2, D.3 |
| ➕ **Rapport de cotes** (*odds ratio*) | Cote avec le facteur, divisée par la cote sans le facteur. 1 = aucun effet ; 2 = cote doublée. | TP2, D.3 |
| ➕ **Effet brut et effet propre** | L'effet brut compare simplement « avec » et « sans » le facteur. L'effet propre est mesuré par le modèle, à autres facteurs égaux. | TP2, D.3 |

**Exemple (TP2)** : la route non goudronnée a un rapport de cotes brut de 3,9, mais un rapport propre de 1,2. Ces routes sont dangereuses surtout parce qu'elles sont rurales et rapides, pas à cause du revêtement lui-même. À l'inverse, la présence d'un deux-roues motorisé a un effet propre (× 8) plus fort que son effet brut (× 4,9).
