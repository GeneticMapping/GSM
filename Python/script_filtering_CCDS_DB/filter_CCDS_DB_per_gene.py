import fileinput

input_filepath = "C:/TG/codigos/python_scripts/script_filtering_CCDS_DB/DBs/CCDS_IDs.txt"
CCDS_gene_pos_filepath = "C:/TG/codigos/python_scripts/script_filtering_CCDS_DB/DBs/CCDS.current.txt"
CCDS_gene_data_filepath = "C:/TG/codigos/python_scripts/script_filtering_CCDS_DB/DBs/CCDS_nucleotide.current.fna"
output_file_path = "C:/TG/codigos/python_scripts/script_filtering_CCDS_DB/outputs/CCDS_data/"

for line in fileinput.input(input_filepath):
        CCDS_ID = line.split()[2]
        gene_name = line.split()[0]

        # grava informacoes sobre a localizacao exata do gene no cromossomo
        output_file = open(str(output_file_path + gene_name + '_pos.txt'), 'w')
        input_file = open(CCDS_gene_pos_filepath, 'r')
        pos_line = input_file.readline()

        while pos_line:
                if CCDS_ID in pos_line:
                        output_file.write(pos_line.split()[0])
                        
                        new_pos_list = []
                        pos_list = pos_line.split('[')[1].split(']')[0].split(', ')
                        for element in pos_list:
                                current_pos = element.split('-')
                                output_file.write('\n' + current_pos[0])
                                output_file.write(' ' + current_pos[1])
                        break
                pos_line = input_file.readline()

        # grava informacoes sobre a area codificante do gene
        output_file = open(str(output_file_path + gene_name + '_GENE.txt'), 'w')
        input_file = open(CCDS_gene_data_filepath, 'r')
        data_line = input_file.readline()

        while data_line:
                if CCDS_ID in data_line:
                        data_line = input_file.readline()
                        while '>' not in data_line:
                                output_file.write(data_line.lower().replace("\n", ""))
                                data_line = input_file.readline()
                        break
                data_line = input_file.readline()
