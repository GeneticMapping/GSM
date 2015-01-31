import shutil
import sys

if int(sys.argv[1]) > 0 and int(sys.argv[1]) < 6:
        external_py_code = str('C:\\TG\\codigos\\python_scripts\\script_excel_input\\excel2text_format_' + sys.argv[1] + '.py')
        execfile(external_py_code)
        
        file_name = "C:/TG/codigos/python_scripts/script_excel_input/excel2text_format_" + str(sys.argv[1]) + "/PatientsData.txt"
        BI_file_name = "C:/TG/codigos/python_scripts/script_excel_input/text2BigInput/PatientsData.txt"

        destination = open(BI_file_name,'wb')

        i = 1 
        while(i < 101):
                shutil.copyfileobj(open(file_name,'rb'), destination)
                i += 1

        destination.close()
else:
        print('argumento: ' + sys.argv[1])
        print('argumento incompativel... tente algo entre 1 e 5 ;)')
