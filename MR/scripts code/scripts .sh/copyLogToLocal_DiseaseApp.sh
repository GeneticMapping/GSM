#!/bin/bash

if [ $# == 3 ]
then
	mkdir -p Logs/"$1"/"$2"GB_"$3"exec
	hadoop dfs -copyToLocal /user/brfilho/output/_logs/history/* ~/Logs/"$1"/"$2"GB_"$3"exec/
fi
