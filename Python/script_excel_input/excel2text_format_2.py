from os import listdir
import xlrd

file = open("C:/TG/codigos/python_scripts/script_excel_input/excel2text_format_2/PatientsData.txt", 'w')
file.close()

# loop de iteracao sobre a tabela excel de cada gene
for f in listdir("C:/TG/codigos/python_scripts/script_excel_input/inputs"):
        excel_file_path = "C:/TG/codigos/python_scripts/script_excel_input/inputs/"+f
        workbook = xlrd.open_workbook(excel_file_path)
        worksheets = workbook.sheet_names()

        string = f.split('-', 1)
        gene_name = string[0]

        for worksheet_name in worksheets:
                worksheet = workbook.sheet_by_name(worksheet_name)
                num_cols = worksheet.ncols
                curr_col = 10
                file_name = ""
                file = open("C:/TG/codigos/python_scripts/script_excel_input/excel2text_format_2/PatientsData.txt", 'a')
                
                # loop de iteracao sobre cada paciente (coluna >= 11)
                while curr_col < num_cols:
                        num_rows = worksheet.nrows
                        curr_row = 1
                        pat_data = ">" + gene_name + "_p" + str(curr_col-9) + "\n"
                        nr_ref = 1
                        
                        while curr_row < num_rows:
                                region_type = worksheet.cell_value(curr_row, 4)
                                cell_value = worksheet.cell_value(curr_row, curr_col)
                                if  region_type == "c":
                                        if len(cell_value) != 1 or cell_value == " ":
                                                cell_value = "n"
                                        pat_data += cell_value
                                        if nr_ref == 3:
                                                pat_data += "\n"
                                                nr_ref = 1
                                        else:   nr_ref += 1
                                curr_row += 1
                                
                        file.write(str(pat_data))
                        curr_col += 1

                file.close()
                        
                # gera arquivo do gene de comparacao
                num_rows = worksheet.nrows
                curr_row = 1
                gen_data = ""
                nr_ref = 1
                
                while curr_row < num_rows:
                        region_type = worksheet.cell_value(curr_row, 4)
                        cell_value = worksheet.cell_value(curr_row, 6)
                        if  region_type == "c":
                                if len(cell_value) != 1 or cell_value == " ":
                                        cell_value = "n"
                                gen_data += cell_value
                                if nr_ref == 3:
                                        gen_data += "\n"
                                        nr_ref = 1
                                else:   nr_ref += 1
                        curr_row += 1

                file_name = gene_name + "_GENE.txt"
                file = open("C:/TG/codigos/python_scripts/script_excel_input/excel2text_format_2/"+file_name, 'w')
                file.write(str(gen_data))
                file.close()
