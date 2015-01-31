#!/bin/bash

for num_maq in 10
do

echo "configurando o cluster - Hadoop utiliza $num_maq maquina(s)"

# restaura configuracoes do Hadoop
sh ./restoreHadoop_DiseaseApp.sh $num_maq

# inicializa o Hadoop
sh ./initHadoop_DiseaseApp.sh

	for tam_entrada in 1 2 5 10 20
	do
		
		echo "copiando entrada de $tam_entrada GB para o HDFS"
		
		#START=$(date +%s)
		
		# copia arquivo de entrada para o HDFS
		sh ./setup_DiseaseApp.sh $tam_entrada
		
		#END=$(date +%s)
		#inputCopyTime=$(( $END - $START ))
		
		echo "executando testes com esta entrada no HDFS"
		# testes rodam 30 vezes
		for i in {1..5}
		do
			# roda a aplicacao MR
			sh ./start_DiseaseApp.sh
			
			# copia log de execucao (considerar nomenclatura para pastas (num_maq) e arquivos (tam_entrada + i))
			sh ./copyLogToLocal_DiseaseApp.sh $num_maq $tam_entrada $i
			
			# remove pasta de output do HDFS
			sh ./removeHadoopOutput_DiseaseApp.sh
		done
		
		echo "fim dos testes - removendo esta entrada do HDFS"
		# remove arquivo de entrada do HDFS
		sh ./removeHadoopInput_DiseaseApp.sh
	done

# para a execucao do Hadoop
sh ./stop_DiseaseApp.sh

done