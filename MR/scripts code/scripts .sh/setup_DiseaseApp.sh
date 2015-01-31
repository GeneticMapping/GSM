#!/bin/bash

if [ $# == 1 ]
then
	echo "HDFS - gerando pasta para input..."
	hadoop dfs -mkdir input
	hadoop dfs -copyFromLocal /home/brfilho/HCPA_App/input/PatientsData_"$1"GB.txt input
fi
