from os import listdir
import fileinput

geneReportDB_path = "C:/TG/codigos/python_scripts/script_filtering_GeneReport_DB/DBs/GeneReport/"
omimDB_file_path = "C:/TG/codigos/python_scripts/script_filtering_GeneReport_DB/DBs/Omim/OmimVarLocusIdSNP.bcp"
output_file_path = "C:/TG/codigos/python_scripts/script_filtering_GeneReport_DB/outputs/"

for f in listdir(geneReportDB_path):
        geneID = f.split('_')[0]
        input_file = open(geneReportDB_path + f, 'r')
        output_file = open(output_file_path + geneID + '.txt', 'w')

        line_iter = input_file.readline()
        line_iter = input_file.readline()
        line_iter = input_file.readline()

        rs_num_list = []
        HGVS_namesList = []
        proteinList = []
        proteinInfo = ''
        codonList = []
        codonInfo = ''
        codonPos = 0
        extra_info = ''
        output_content = ''
        extra_ind = 0

        clinical_significance = ['unknown', 'untested', 'probable-pathogenic', 'pathogenic', 'drug-response', 'histocompatibility', 'other']
        allele_origin = ['somatic', 'germline']
        
        #str_filter = ["pathogenic", "germline", "somatic"]
        #if any(s in line_iter for s in str_filter):

        while line_iter:
                #if any(s in line_iter for s in str_filter):
                if any(s in line_iter for s in clinical_significance) or any(s in line_iter for s in allele_origin):
                        line_iter = line_iter.split()

                        if 'NC_' in line_iter[3]:
                                extra_ind = 1
                                        
                        else:
                                extra_ind = 0

                        rs_num = line_iter[4+extra_ind]

                        if rs_num not in rs_num_list:
                                HGVS_namesList = line_iter[2+extra_ind].split(';')
                                for element in HGVS_namesList:
                                        if 'NM_' in element and 'del' not in element and '+' not in element:
                                                codonList.append(element.split(':c.')[1])
                                        if 'NP_' in element and 'del' not in element and '+' not in element:
                                                proteinList.append(element.split(':p.')[1])

                                if len(codonList) >= 2:
                                        codonPos = 1 + (int((codonList[1])[:-3])/3)
                                        codonMutPos = int((codonList[1])[:-3])%3
                                        if codonMutPos == 0:
                                                codonMutPos = 3
                                        codonInfo = str(codonPos) + ' ' + str(codonMutPos) + ' ' + (codonList[1])[-3:]
                                        proteinInfo = proteinList[1].replace(str(codonPos),'-')
                                else:
                                        if len(codonList) == 1:
                                                codonPos = 1 + (int((codonList[0])[:-3])/3)
                                                codonMutPos = int((codonList[0])[:-3])%3
                                                if codonMutPos == 0:
                                                        codonMutPos = 3
                                                codonInfo = str(codonPos) + ' ' + str(codonMutPos) + ' ' + (codonList[0])[-3:]
                                                proteinInfo = proteinList[0].replace(str(codonPos),'-')
                                        
                                output_content = codonInfo + ' ' + proteinInfo + ' '
                                proteinList = []
                                proteinInfo = 'proteinInfo_not_found'
                                codonList = []
                                codonInfo = 'codonInfo_not_found'
                                
                                extra_info = line_iter[6+extra_ind] + line_iter[7+extra_ind] + line_iter[8+extra_ind]
                                output_content += rs_num + ' '
                                rs_num_list.append(rs_num)

                                for cs in clinical_significance:
                                        if cs in extra_info:
                                                output_content += cs + ' '
                                                break
                                for ao in allele_origin:
                                        if ao in extra_info:
                                                output_content += ao + ' '
                                                break
                                if "e-" in line_iter[6+extra_ind]:
                                        output_content += line_iter[6+extra_ind] + ' '

                                # search for OMIM reference
                                omim_input_file = open(omimDB_file_path, 'r')
                                omim_iter = omim_input_file.readline()
                                omim_info = ''

                                while omim_iter:
                                        if rs_num in omim_iter:
                                                omim_info = omim_iter.split()
                                                output_content += 'http://omim.org/entry/' + omim_info[0] + '#' + omim_info[2] + ' '
                                                break
                                        omim_iter = omim_input_file.readline()

                                omim_input_file.close()

                                output_content += '\n'
                                output_file.write(output_content)
                                
                line_iter = input_file.readline()

        output_file.close()
        input_file.close()

        
