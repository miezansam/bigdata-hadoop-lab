# TP2 — Déploiement d'un cluster Amazon EMR

![Amazon EMR](https://img.shields.io/badge/Amazon-EMR_6.15.0-8C4FFF?logo=amazonaws&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-3.4.1-E25A1C?logo=apachespark&logoColor=white)
![Hive](https://img.shields.io/badge/Hive-3.1.3-FDEE21?logo=apachehive&logoColor=black)
![YARN](https://img.shields.io/badge/YARN-Resource_Management-lightgrey)

Reproduction du pipeline du [TP1](../01-hadoop-ec2-installation) sur une infrastructure Hadoop **managée et multi-nœuds** via Amazon EMR, afin de comparer l'approche manuelle à l'approche gérée par AWS.

📄 Rapport complet : [`Rapport_EMR_Miezan_Sam_YAO.pdf`](./Rapport_EMR_Miezan_Sam_YAO.pdf)

## Infrastructure

- Cluster EMR 6.15.0 managé par AWS, région `af-south-1`
- Topologie : 1 nœud primaire (master) + 1 nœud principal (core) + 1 nœud de tâche (task)

## Stack

Hadoop 3.3.6, Hive 3.1.3, Spark 3.4.1, Livy 0.7.1, le tout préinstallé et géré automatiquement par EMR, sans configuration XML manuelle.

## Pipeline reproduit

1. Ingestion S3 → zones Raw/Processed/Curated sur HDFS
2. ETL Spark : conversion CSV → Parquet, partitionné par opérateur, soumis avec `--master yarn`
3. 3 analyses Spark SQL : volume par opérateur, distribution des statuts, top 5 des zones expéditrices

## Preuve du parallélisme distribué

Exécution répartie sur plusieurs exécuteurs (driver + 2 executors, 8 cœurs), confirmée via :
- les journaux Spark
- le Spark History Server
- le YARN ResourceManager

Temps de calcul cumulé de **1,2 min** pour seulement **34 s** de temps réel écoulé, preuve concrète du parallélisme.

## EC2 manuel vs EMR managé

| Critère | EC2 manuel (TP1) | Amazon EMR (ce TP) |
|---|---|---|
| Installation | Manuelle (configuration XML) | Automatique |
| Nœuds | 1 (pseudo-distribué) | Plusieurs (réellement distribué) |
| Gestion mémoire | Manuelle (OOM à corriger) | Automatique |
| Accès S3 | Clés manuelles | Rôle IAM automatique |
| Scalabilité | Aucune | Ajout/retrait de nœuds à la demande |
| Exécution Spark | `--master local[2]` | `--master yarn` |
