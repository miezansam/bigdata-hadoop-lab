# TP1 — Installation Hadoop sur AWS EC2 (mode pseudo-distribué)

![Hadoop](https://img.shields.io/badge/Hadoop-3.4.3-66CCFF?logo=apachehadoop&logoColor=white)
![AWS EC2](https://img.shields.io/badge/AWS-EC2-FF9900?logo=amazonaws&logoColor=white)
![Ubuntu](https://img.shields.io/badge/Ubuntu-Server_26.04_LTS-E95420?logo=ubuntu&logoColor=white)

Déploiement manuel d'un cluster Hadoop en mode pseudo-distribué sur une instance EC2 unique, première étape du travail de fond du module avant l'introduction de Spark et d'EMR.

📄 Rapport complet : [`rapport_hadoop_aws_ec2_miezan_sam_yao.pdf`](./rapport_hadoop_aws_ec2_miezan_sam_yao.pdf)

## Infrastructure

- Instance EC2 `t3.micro` (2 vCPU, 1 Go RAM), Ubuntu Server 26.04 LTS, région `af-south-1`
- Stockage 20 Go SSD gp3

## Stack

- Hadoop 3.4.3, Java 11 (OpenJDK)
- HDFS + YARN configurés manuellement (4 fichiers XML)

## Difficulté rencontrée et correctif

Le **OOM Killer** du noyau Linux tuait le DataNode et le ResourceManager faute de RAM disponible sur l'instance `t3.micro`. Diagnostic effectué via `free -h` et `dmesg`.

Correctifs appliqués :
- Ajout de 2 Go de swap
- Réduction des heaps JVM à 256 Mo
- Limitation de la mémoire des conteneurs YARN (768 Mo max)

## Résultat

Cluster stable avec les 5 daemons actifs : NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager. Test HDFS écriture/lecture validé.

Ce cluster sert de base à l'ingestion du jeu de données Mobile Money utilisé dans le [TP3 (Data Lake HDFS + Spark)](../03-datalake-hdfs-spark-mobile-money).
