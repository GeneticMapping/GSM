from os import listdir
import fileinput

EnsemblDB_path = "C:/TG/codigos/python_scripts/script_filtering_CCDS_DB/DBs/Ensembl/DB_polyphen_and_sift"
CCDS_gene_data_filepath = "C:/TG/codigos/python_scripts/script_filtering_CCDS_DB/outputs/CCDS_data/"
output_file_path = "C:/TG/codigos/python_scripts/script_filtering_CCDS_DB/outputs/Ensembl_data/"

for f in listdir(CCDS_gene_data_filepath):
        if '_pos' in f:
                geneID = f.split('_')[0]

                input_file = open(CCDS_gene_data_filepath + f, 'r')
                output_file = open(output_file_path + geneID + '.txt', 'w')
                
                line_iter = input_file.readline()
                chr_number = str(line_iter).rstrip('\n')
                coding_info = []
                cod_counter = 0

                line_iter = input_file.readline()
                while line_iter:
                        line_iter = line_iter.split()
                        coding_info = range(int(line_iter[0]), int(line_iter[1]) + 1)
                        
                        for line in fileinput.input(EnsemblDB_path + chr_number + '.txt'):
                                ws_splited_line = line.split()
                                if int(ws_splited_line[3]) in coding_info:
                                        cod_number = cod_counter + int(ws_splited_line[3]) - int(line_iter[0]) + 1
                                        mutation_pos = cod_number % 3
                                        if mutation_pos == 0:
                                                mutation_pos = 3
                                        codon_number = int(cod_number / 3) + 1
                                        output_file.write(str(codon_number) + ' ' + str(mutation_pos) + ' ')
                                        
                                        splited_line = line.rstrip('\n').split(';')
                                        for element in splited_line:
                                                if 'variant_peptide' in element:
                                                        output_file.write('variant_peptide ' + element.split()[1] + ' ')
                                                if 'polyphen_prediction' in element:
                                                        output_file.write('polyphen_prediction ' + element.split()[2] + ' ')
                                                if 'sift_prediction' in element:
                                                        output_file.write('sift_prediction ' + element.split()[2] + ' ')
                                                if 'Reference_seq' in element:
                                                        output_file.write('Reference_seq ' + element.split('=')[1] + ' ')
                                                if 'Variant_seq' in element:
                                                        output_file.write('Variant_seq ' + element.split('=')[1] + ' ')
                                                if 'reference_peptide' in element:
                                                        output_file.write('reference_peptide ' + element.split('=')[1] + ' ')
                                                if 'Dbxref' in element:
                                                        output_file.write('Dbxref ' + element.split('=')[1])
                                        
                                        output_file.write('\n')

                        cod_counter += int(line_iter[1]) - int(line_iter[0]) + 1
                        line_iter = input_file.readline()

                output_file.close()
                input_file.close()     
