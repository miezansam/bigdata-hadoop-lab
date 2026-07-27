# Big Data 

![Hadoop](https://img.shields.io/badge/Hadoop-3.4.3-66CCFF?logo=apachehadoop&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-3.4.1_/_3.5.1-E25A1C?logo=apachespark&logoColor=white)
![AWS EC2](https://img.shields.io/badge/AWS-EC2-FF9900?logo=amazonaws&logoColor=white)
![Amazon EMR](https://img.shields.io/badge/Amazon-EMR-8C4FFF?logo=amazonaws&logoColor=white)
![HDFS](https://img.shields.io/badge/HDFS-Distributed_Storage-blue)
![Parquet](https://img.shields.io/badge/Apache_Parquet-Columnar_Storage-50ABF1)

Ce dépôt regroupe l'ensemble des travaux pratiques réalisés dans le cadre du module **Big Data**, Master 1 DATA-IA, Université Polytechnique de Bingerville (UPB).

## Auteur

**YAO MIÉZAN SAM WILLIAM** , Master 1 DATA-IA, Université Polytechnique de Bingerville
**Enseignant** : Dr BOBET GOUALO      
**Année académique** : 2025-2026

Tous les TP partagent un fil conducteur commun : un jeu de données de **100 000 transactions Mobile Money** simulant l'activité de quatre opérateurs ivoiriens (Wave, Orange Money, MTN CI, Moov Africa), traité à travers différentes briques de l'écosystème Hadoop/Spark, sur différentes infrastructures (EC2 manuel, EMR managé).

## Projets du module

| # | Projet | Description | Auteur(s) |
|---|---|---|---|
| 01 | [Installation Hadoop sur AWS EC2](./01-hadoop-ec2-installation) | Déploiement manuel d'un cluster Hadoop pseudo-distribué sur une instance EC2 unique | YAO MIÉZAN SAM WILLIAM |
| 02 | [Cluster Amazon EMR](./02-amazon-emr-cluster) | Reproduction du pipeline sur un cluster EMR managé multi-nœuds, avec comparaison à l'approche manuelle | YAO MIÉZAN SAM WILLIAM |
| 03 | [Data Lake HDFS + Spark](./03-datalake-hdfs-spark-mobile-money) | Architecture Data Lake complète (Raw/Processed/Curated), pipeline ETL PySpark, analyses Spark SQL et benchmark CSV vs Parquet | GNAPIÉ ARIEL NATHAN, SYLLA BAZOUMANA, YAO MIÉZAN SAM WILLIAM |

## Structure du dépôt

```
bigdata-upb-dr-bobet/
├── 01-hadoop-ec2-installation/
│   ├── README.md
│   └── rapport_hadoop_aws_ec2_miezan_sam_yao.pdf
├── 02-amazon-emr-cluster/
│   ├── README.md
│   └── Rapport_EMR_Miezan_Sam_YAO.pdf
└── 03-datalake-hdfs-spark-mobile-money/
    ├── README.md
    ├── rapport_datalake_hadoop.pdf
    └── docs/
        └── architecture-datalake.png
```

## Vue d'ensemble : EC2 manuel vs EMR managé

Les deux premiers TP appliquent le même objectif (cluster Hadoop fonctionnel) sur deux approches différentes :

| Critère | EC2 manuel (TP1) | Amazon EMR (TP2) |
|---|---|---|
| Installation | Manuelle (configuration XML) | Automatique |
| Nœuds | 1 (pseudo-distribué) | Plusieurs (réellement distribué) |
| Gestion mémoire | Manuelle (OOM à corriger) | Automatique |
| Accès S3 | Clés manuelles | Rôle IAM automatique |
| Scalabilité | Aucune | Ajout/retrait de nœuds à la demande |
| Exécution Spark | `--master local[2]` | `--master yarn` |

Le TP3 approfondit ensuite la couche applicative (Data Lake, ETL, analytique) sur cette même base technique.

## Outils et technologies

AWS (EC2, EMR, S3, IAM), Hadoop, HDFS, YARN, Apache Spark, PySpark, Spark SQL, Apache Parquet, PuTTY/SSH, Linux (Ubuntu).

## Licence

Projets académiques à usage pédagogique.
