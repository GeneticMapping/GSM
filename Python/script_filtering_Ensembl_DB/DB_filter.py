"""

OBS.:

itera sobre arquivos de entrada(DB), gerando os devidos
arquivos de saida(DB filtrado) que serao utilizados na aplicacao Hadoop-MR

"""

from os import listdir
import fileinput

file_path = "C:/TG/codigos/python_scripts/script_filtering_Ensembl_DB/outputs/DB_polyphen_and_sift.txt"
file = open(file_path, 'w')

for f in listdir("C:/TG/codigos/python_scripts/script_filtering_Ensembl_DB/inputs"):
        arq_name = "C:/TG/codigos/python_scripts/script_filtering_Ensembl_DB/inputs/"+f
        for line in fileinput.input(arq_name):
                if ('clinical_significance=probable-pathogenic' in line or 'clinical_significance=pathogenic' in line) and ('sift' in line or 'polyphen' in line):
                        file.write(line)

file.close()
