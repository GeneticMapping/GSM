#!/bin/bash

if [ $# == 1 ]
then
	cp /home/brfilho/HCPA_App/hadoop-modificado/"$1"/* /etc/hadoop/
fi

n_maq=`expr $1 + 0`

for node_num in 0 1 2 3 4 6 8 9 10 13 14 15 16 17 18
do
	if [ $n_maq -gt 0 ]
	then
		scp /etc/hadoop/*.xml compute-0-"$node_num":/etc/hadoop/
		scp /etc/hadoop/slaves compute-0-"$node_num":/etc/hadoop/
		scp /etc/hadoop/masters compute-0-"$node_num":/etc/hadoop/
	fi
	
	n_maq=`expr $n_maq - 1`
done

#min_n=0
#max_n=21
#
#while [ "$min_n" -le "$max_n" ]; do
#        scp /etc/hadoop/*.xml compute-0-${min_n}:/etc/hadoop/
#    scp /etc/hadoop/slaves compute-0-${min_n}:/etc/hadoop/
#    scp /etc/hadoop/masters compute-0-${min_n}:/etc/hadoop/
#        min_n=`expr $min_n \+ 1`
#done