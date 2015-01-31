#!/bin/bash

hadoop namenode -format
/usr/sbin/start-dfs.sh
/usr/sbin/start-mapred.sh

echo "HDFS - gerando pasta para Genes de Referencia..."
hadoop dfs -mkdir generef
hadoop dfs -copyFromLocal /home/brfilho/HCPA_App/generef/*.* generef

echo "HDFS - gerando pasta para a Base de Dados Ensembl..."
hadoop dfs -mkdir db
hadoop dfs -copyFromLocal /home/brfilho/HCPA_App/db/*.* db
