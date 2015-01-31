import fileinput

file_path = "C:/TG/codigos/python_scripts/script_filtering_Ensembl_DB/inputs/DB_polyphen_and_sift.txt"
output_file_path = "C:/TG/codigos/python_scripts/script_filtering_Ensembl_DB/outputs/DB_polyphen_and_sift"

for line in fileinput.input(file_path):
        chr_ID = line.split()[0]
        file = open(str(output_file_path + chr_ID + '.txt'), 'ab+')
        file.write(line)
