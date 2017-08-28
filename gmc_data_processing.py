# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 16:58:46 2017

@author: Alex
"""

import zipfile
import csv
import glob
import re
import time
import pandas

'''Code for processing raw Geisinger data into format for Murph's data frame code. Produces files named PT####-ENC####.csv,
with data in format Name, Value, Date. Most files are processed from zipped form.'''
'''some functions could be updated to use the csv reader module'''

def filter_faster(vital_filename, csv_name=None, extra_line = False):
    '''function used to filter and format ssdb_demogaphics.zip, ssdb_problem_list.zip'''  
    #regular expression to handle instances of commas within cells (would be handled by csv reader also)
    pattern = re.compile(r"[,](?!\s)")
    with zipfile.ZipFile(vital_filename, 'r') as zf:
        with zf.open(csv_name, 'r') as vf:
            header = str(vf.readline())[2:-5]
            header = re.split(pattern,header)
            if extra_line == True:
                vf.readline()
            first_line = str(vf.readline())[2:-5]
            first_line = re.split(pattern, first_line)
            print(first_line)
            curr_pat = first_line[0]
            write_string = ''
            for ii in range(len(first_line)):
                try:
                    write_string += header[ii]+','+first_line[ii]+','+'None'+'\n'
                except IndexError:
                    with open('index_error.txt', 'a') as ie:
                        ie.writelines(line)
            for line in vf:
                line = str(line)
                line = line[2:-5]
                line = re.split(pattern, line)
                if line[0] == curr_pat:
                    try:
                        for ii in range(len(line)):
                            try:
                                write_string += header[ii]+','+line[ii]+','+'None'+'\n'
                            except IndexError:
                                with open('index_error.txt', 'a') as ie:
                                    ie.writelines(line)
                        curr_pat = line[0]
                    except:
                        print(curr_pat)
                else:
                    try:
                        for file in glob.glob('Encounter Files\\'+curr_pat+'*.csv'):
                            with open(file, 'a') as f:
                                f.write(write_string)
                        write_string = ''
                        for ii in range(len(line)):
                            try:
                                write_string += header[ii]+','+line[ii]+','+'None'+'\n'
                            except IndexError:
                                with open('index_error.txt', 'a') as ie:
                                    ie.writelines(line)
                        curr_pat = line[0]

                    except:
                        print(line[0])
            for file in glob.glob('Encounter Files\\'+curr_pat+'*.csv'):
                with open(file, 'a') as f:
                    f.write(write_string)
    print('done')


def filter_vital(vital_filename, dt_inx, val_inx, name_inx, csv_name=None):
    '''original function to process ssdb_vitals.zip, made obsolete by filter_faster'''
    #'alexs_encounters.txt' is a text file containing the encounter numbers of all the inpatient encounters
    with open('alexs_encounters.txt') as ef:
        enc_list = ef.readlines()
        for ii in range(len(enc_list)):
            enc_list[ii] = enc_list[ii][:-1]
        #print(enc_list)
    pattern = re.compile(r"[,](?!\s)")
    with zipfile.ZipFile(vital_filename, 'r') as zf:
        with zf.open(csv_name, 'r') as vf:
            write_string = ''
            header = str(vf.readline())[2:-5]
            first_line = str(vf.readline())[2:-5]
            first_line = re.split(pattern, first_line)
            curr_enc = first_line[0]
            write_string += first_line[name_inx]+','+first_line[val_inx]+','+first_line[dt_inx]+'\n'
            for line in vf:
                line = str(line)
                line = line[2:-4]
                line = re.split(pattern, line)
                if line[0] == curr_enc:
                    try:
                        pat_data = line[name_inx]+','+line[val_inx][:-1]+','+ line[dt_inx]+'\n'
                        write_string += pat_data
                        curr_pat = line[1]
                    except:
                        print(line[1]+'-'+curr_enc)
                else:
                    try:
                        if curr_enc in enc_list:
                            with open('Encounter Files\\'+curr_pat+'-'+curr_enc+'.csv', 'a') as f:
                                f.write(write_string)
                        pat_data = line[name_inx]+','+line[val_inx][:-1]+','+line[dt_inx]+'\n'
                        write_string = pat_data
                        curr_enc = line[0]



                    except:
                        print(line[1]+'-'+curr_enc)
            if curr_enc in enc_list:
                with open('Encounter Files\\'+curr_pat+'-'+curr_enc+'.csv', 'a') as f:
                    f.write(write_string)
    print('done')


def filter_categorical_data(filename,csv_name=None, enc=False, enc_inx = 1, pat_inx = 0,start=1):
    '''function to filter ssdb_social_history.zip, ssdb_encounters.zip, and ssdb_cohort_admits.zip. These files required a separate
    function to process them as they were formatted differently from the "vitals format"'''
    with open('alexs_encounters.txt') as ef:
        enc_list = ef.readlines()
        for ii in range(len(enc_list)):
            enc_list[ii] = enc_list[ii][:-1]
#        print(enc_list)
    pattern = re.compile(r"[,](?!\s)")
    with zipfile.ZipFile(filename, 'r') as zf:
        with zf.open(csv_name, 'r') as vf:
            header = str(vf.readline())[2:-5]
            header= re.split(pattern, header)
            #print(header)
            for line in vf:
                line = str(line)
                line = line[2:-5]
                line = re.split(pattern, line)
                #print(line)
                if enc == False:
                    curr_pat = line[pat_inx]
                    for file in glob.glob('Encounter Files\\'+curr_pat+'*.csv'):
                        try:
                            with open(file, 'a') as f:
                                write_string = ''
                                for ii in range(len(line)):
                                    try:
                                        write_string += header[ii]+','+line[ii]+','+'None'+'\n'
                                    except IndexError:
                                        with open('index_error.txt', 'a') as ie:
                                            ie.writelines(line)


                                f.write(write_string)
                        except:
                            print(line[1])

                else:

                    curr_pat = line[pat_inx]
                    curr_enc = line[enc_inx]
                    #print(curr_enc)
                    if curr_enc in enc_list:
                        try:
                            with open('Encounter Files\\'+curr_pat+'-'+curr_enc+'.csv', 'a') as f:
                            #with open(curr_pat+'-'+curr_enc+'.csv', 'a') as f:
                                write_string = ''
                                for ii in range(len(line)):

                                    try:
                                        write_string+= header[ii]+','+line[ii]+','+'None'+'\n'
                                    except IndexError:
                                        with open('index_error.txt', 'a') as ie:
                                            ie.writelines(line)

                                f.write(write_string)

                        except:
                            print(line[1]+'-'+curr_enc)

    print('done')


def no_enc_num(filename1, filename2,nm_inx, val_inx, dt_inx, csv_name1=None,csv_name2=None):
    '''function to deal with ssdb_lab_orders where the encounter number is not listed. Order number is used to relate orders to results. 
    Made obsolete by write_2 (ended up being slow/ possibly incorrect)'''
    with open('alexs_encounters.txt') as ef:
        enc_list = ef.readlines()
        for ii in range(len(enc_list)):
            enc_list[ii] = enc_list[ii][:-1]
    pattern = re.compile(r"[,](?!\s)")
    with zipfile.ZipFile(filename1, 'r') as zf:
        with zf.open(csv_name1, 'r') as vf:
            enc_dict = {}
            header = vf.readline()
            first_line = str(vf.readline())[2:-5]
            first_line = re.split(pattern, first_line)
            enc_dict[first_line[0]]=first_line[1]
            for line in vf:
                line = str(line)[2:-5]
                line = re.split(pattern, line)
                enc_dict[line[0]]=line[1]
                #print(line[1])
    with zipfile.ZipFile(filename2, 'r') as zf:
        with zf.open(csv_name2, 'r') as rf:
            header = rf.readline()
            first_line = str(rf.readline())[2:-5]
            first_line = re.split(pattern, first_line)
            curr_ord_id = first_line[0]
            write_string = ''
            for line in rf:
                line = str(line)[2:-5]
                line = re.split(pattern,line)
                if line[0] == curr_ord_id:
                    try:

                        write_string+=line[nm_inx]+','+line[val_inx]+','+line[dt_inx]+'\n'
                        enc_id = enc_dict[curr_ord_id]
                        #print(enc_id)
                        pat_id = line[1]
                    except KeyError:
                        with open('key_error.txt', 'a') as ke:
                            ke.write(line[0]+', ')

                else:
                    try:
                        if enc_id in enc_list:
                            with open('Encounter Files\\'+pat_id+'-'+enc_id+'.csv', 'a') as f:
                                f.write(write_string)
                        curr_ord_id = line[0]
                        write_string = ''
                        write_string += line[nm_inx]+','+line[val_inx]+','+line[dt_inx]+'\n'
                        enc_id = enc_dict[curr_ord_id]
                        pat_id = line[1]
                    except KeyError:
                        with open('key_error.txt', 'a') as ke:
                            ke.write(line[0]+', ')
            enc_id = enc_dict[curr_ord_id]
            pat_id = line[1]
            if enc_id in enc_list:
                with open('Encounter Files\\'+pat_id+'-'+enc_id+'.csv', 'a') as f:
                    f.write(write_string)

    print('done')


def filter_vitals_no_zip(dt_inx, val_inx, name_inx, csv_name):
    start = time.time()
    '''processes vitals "type" files that have been unzipped. Used for the sorted version of lab_results, could be used for other files'''
    with open('alexs_encounters.txt') as ef:
        enc_list = ef.readlines()
        for ii in range(len(enc_list)):
            enc_list[ii] = enc_list[ii][:-1]
        #print(enc_list)


    with open(csv_name, 'r') as vf:
        reader = csv.reader(vf)
        write_string = []
        first_line = next(reader)
        print(first_line)
        curr_enc = first_line[-1]
        write_string += [[first_line[name_inx],first_line[val_inx],first_line[dt_inx]+'\n']]
        for line in reader:
            if line[-1] == curr_enc:
                try:
                    pat_data = [[line[name_inx],line[val_inx],line[dt_inx]+'\n']]
                    write_string += pat_data
                    curr_pat = line[1]
                except:
                    print(line[1]+'-'+curr_enc)
            else:
                try:
                    if curr_enc in enc_list:
                        with open('Encounter Files 2\\'+curr_pat+'-'+curr_enc+'.csv', 'a', newline = '\n') as f:
                            writer = csv.writer(f)
                            writer.writerows(write_string)
                    pat_data = [[line[name_inx],line[val_inx],line[dt_inx]+'\n']]
                    write_string = pat_data
                    curr_enc = line[-1]



                except:
                    print(line[1]+'-'+curr_enc)
        if curr_enc in enc_list:
            with open('Encounter Files 2\\'+curr_pat+'-'+curr_enc+'.csv', 'a') as f:
                f.write(write_string)
    end = time.time()
    print(end-start)
    print('done')


def write_2(zip1, csv_1, csv_2, out_csv):
    '''improved way of handling lab_results/orders problem. Writes a new file where encounter numbers are attached to existing lab results file. 
    lab_resuls had to be unzipped for this. This new file was then sorted using sort_data by encounter number and fed into filter_vitals_no_zip'''
    start = time.time()
    #pattern1 = re.compile(r"(,\"|\",)")
    with open('alexs_encounters.txt') as ef:
        enc_list = ef.readlines()
        for ii in range(len(enc_list)):
            enc_list[ii] = enc_list[ii][:-1]
    pattern = re.compile(r"[,](?!\s)")
    with zipfile.ZipFile(zip1, 'r') as zf:
        with zf.open(csv_1, 'r') as vf:
            enc_dict = {}
            header = vf.readline()
            first_line = str(vf.readline())[2:-5]
            first_line = re.split(pattern, first_line)
            enc_dict[first_line[0]]=first_line[1]
            for line in vf:
                line = str(line)[2:-5]
                line = re.split(pattern, line)
                enc_dict[line[0]]=line[1]
                #print(line[1])
    with open(csv_2, 'r') as rf:
        reader = csv.reader(rf)
        with open(out_csv, 'w', newline='\n') as wf:
            writer = csv.writer(wf)
            for line in reader:
                try:
                    line += [enc_dict[line[0]]]
                    #     #print(line)
                    writer.writerow(line)
                except KeyError:
                    with open('key_error.txt', 'a') as keyf:
                        keyf.write(line[0])
    end = time.time()
    print(end-start)

def sort_data(filename1,filename2, sort_index):
    '''simple sort function used to sort unified encounter-lab results file by encounter number'''
    start = time.time()
    data_frame = pandas.read_csv(filename1, header=0)
    data_frame.sort_values(sort_index)
    data_frame.to_csv(filename2, index = False)
    end=time.time()
    print(end-start)

def find_enc_dttm(directory):
    '''finds the start and end dates for each encounter, and places them in a dictionary'''
    time_dict = {}
    file_list = glob.glob(directory)
    for file in file_list:
        pt,enc=split_name(file)
        with open(file, 'r') as f:
            reader = csv.reader(f)
            try:
                for x in range(6):
                    next(reader)
                start_dt = next(reader)[1]
                start_dt = format_dates(start_dt)
                end_dt = next(reader)[1]
                end_dt = format_dates(end_dt)
            except StopIteration:
                with open('ItError.txt', 'a') as ef:
                    ef.write(pt+'-'+enc+' ')
        if start_dt != '' and end_dt != '':
            if pt not in time_dict.keys():
                time_dict[pt] = [(enc, start_dt, end_dt)]
            else:
                time_dict[pt] += [(enc, start_dt, end_dt)]
    return time_dict
    
def get_mech_vent(filename):
    '''creates a dictionary relating patients to dates on which mechanical ventilation was administered'''
    mv_dict = {}
    codes = ['94002', '94656', '94003', '94657', '94660']
    with open(filename) as f:
        try:
            reader=csv.reader(f)
            next(reader)
            for line in reader:
                pat = line[0]
                cpt = line[2]
                if cpt in codes:
                    if pat not in mv_dict.keys():
                        mv_dict[pat] = [line[1]]
                    else:
                        mv_dict[pat] += [line[1]]
        except StopIteration:
            with open('ItError.txt', 'a'):
                ef.write(filename+' ')
                    
    return mv_dict
def convert_dts(mv_dict, dt_dict):
    '''converts string format dates to datetime python objects'''
    for pt in dt_dict.keys():
        dt_list = dt_dict[pt]
        dt_list = [(x[0], time.strptime(x[1], '%d%b%Y'), time.strptime(x[2], '%d%b%Y')) for x in dt_list]
        dt_dict[pt] = dt_list
    for pt in mv_dict.keys():
        mv_dts = mv_dict[pt]
        try:
            mv_dts = [time.strptime(x, '%d%b%Y') for x in mv_dts]
            mv_dict[pt] = mv_dts
        except ValueError:
            with open('dt_error_file.txt', 'a') as f:
                f.writelines(mv_dict[pt])
        
    return dt_dict, mv_dict

def is_enc_mv(mv_dict, dt_dict):
    '''matches encounter numbers with mechanical ventilation events occurring during the encounter '''
    tf_dict = {}
    dt_dict, mv_dict = convert_dts(mv_dict, dt_dict)
    for pt in dt_dict.keys():
        dt_list = dt_dict[pt]
        try:
            mv_dts = mv_dict[pt]
            #print(mv_dts)
        except KeyError:
            mv_dts = None
        if mv_dts != None:
            for dd in mv_dts:
                for xx in dt_list:
                    #print(xx[1])
                    #print(xx[2])
                    if dd >= xx[1] and dd <= xx[2]:
                        #print(time.strftime('%d%b%Y', dd))
                        if pt+'-'+xx[0] in tf_dict.keys():
                            tf_dict[pt+'-'+xx[0]] += [time.strftime('%d%b%Y', dd)]
                        else:
                            tf_dict[pt+'-'+xx[0]] = [time.strftime('%d%b%Y', dd)]
                        #print('yes')
    return tf_dict
def format_dates(date):
    '''helper for mech vent procedure'''
    date = list(date)
    count = 0
    formatted = ''
    try:
        while date[count] != ':':
            formatted += date[count]
            count+=1
    except IndexError:
        formatted = ''
    return formatted
def split_name(name):
    '''helper mech vent'''
    pt = ''
    count1 = 0
    name = list(name)
    while name[count1] != '\\':
        count1+=1
    name = name[count1+1:]
    count2 = 0
    while name[count2] != '-':
        pt+=name[count2]
        count2 += 1
    name = name[count2+1:]
    enc = ''
    count3 = 0
    while name[count3] != '.':
        enc += name[count3]
        count3+=1
    return pt, enc

def split_name_2(name):
    '''helper mech vent'''
    pt = ''
    count1 = 0
    name = list(name)
    while name[count1] != '\\':
        count1+=1
    name = name[count1+1:]
    count2 = 0
    while name[count2] != '.':
        pt += name[count2]
        count2+=1
    return pt

def mv_main(directory, mv_filename):
    '''mech vent main function'''
    dt_dict = find_enc_dttm(directory)
    mv_dict = get_mech_vent(mv_filename)
    tf_dict = is_enc_mv(mv_dict, dt_dict)
    write_to_file(directory, tf_dict)
    
def write_to_file(directory, tf_dict):
    '''writes mech vent events and times to the appropriate file'''
    file_list = glob.glob(directory)
    for file in file_list:
        with open(file, 'a', newline='\n') as f:
            writer=csv.writer(f)
            f_name = split_name_2(file)
            try:
                tf = tf_dict[f_name]
                for i in tf:
                    writer.writerow(['Mech Vent', 'True', i+'\n'])
            except KeyError:
                with open('keyerror.txt', 'a') as ef:
                    ef.write(f_name+ ' ')
            
            
def find_fluids(filename):
    name_set = set()
    with open(filename, 'r') as f:
        reader=csv.reader(f)
        for line in reader:
            name_set.add(line[17])
            
    with open('fluid_names.csv', 'w') as ff:
        for i in name_set:
            ff.write(i+'\n ')

def get_fluids(filename):
    '''adds fluid intake and presence of vasopressors to encounter files'''
    vasopress = ['VASOPRESSIN 0.4 UNITS/ML PEDIATRIC INFUSION', 'VASOPRESSIN INFUSION ONE-STEP MED ONLY', 'VASOPRESSIN 20 UNITS IN D5W 50ML', 'VASOPRESSIN 20 UNIT/ML IJ SOLN', 'VASOPRESSIN 20 UNIT/ML IV SOLN', 'VASOPRESSIN 40 UNITS IN 100 ML INFUSION']
    with open('alexs_encounters.txt') as ef:
        enc_list = ef.readlines()
        for ii in range(len(enc_list)):
            enc_list[ii] = enc_list[ii][:-1]
        #print(enc_list)
    fluids_dict = {}
    with open('final_fluids.csv', 'r') as ff:
        f_reader = csv.reader(ff)
        for line in f_reader:
            fluids_dict[line[0][1:]] = line[1]
    #print(fluids_dict.keys())
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        firstline = next(reader)
        curr_enc = firstline[1]
        curr_pat = firstline[0]
        write_string = []
#        if firstline[17] in fluids_dict.keys():
#            if firstline[8] != '' and firstline[8] != None:
#                write_string+= [['Fluid Intake', firstline[8], firstline[4]+'\n']]
#                
#            else:
#                if fluids_dict[firstline[17]] != 'None':
#                    write_string+= [['Fluid Intake', fluids_dict[firstline[17]], firstline[4]+'\n']]
#                
        if firstline[17] in vasopress:
            write_string +=  [['Vasopressin', 'True', firstline[4]+'\n']]
        for line in reader:
            
            if line[1] == curr_enc:
                
#                if line[17] in fluids_dict.keys():
#                    print("true")
#                    curr_pat = line[0]
#                    if line[8] != '' and line[8] != None:
#                        write_string+= [['Fluid Intake', line[8], line[4]+'\n']]
#                    else:
#                        if fluids_dict[line[17]] != 'None':
#                            write_string+= [['Fluid Intake', fluids_dict[line[17]], line[4]+'\n']]
                if line[17] in vasopress:
                    write_string +=  [['Vasopressin', 'True', line[4]]]
            else:

                if curr_enc in enc_list:
        
                    with open('Encounter Files 2\\'+curr_pat+'-'+curr_enc+'.csv', 'a', newline = '\n') as f:
                        writer = csv.writer(f)
                        writer.writerows(write_string)
                write_string = []
#                if line[17] in fluids_dict.keys():
#                    if line[8] != '' and line[8] != None:
#                        write_string+= [['Fluid Intake', line[8], line[4]+'\n']]
#                    else:
#                        if fluids_dict[line[17]] != 'None':
#                            write_string+= [['Fluid Intake', fluids_dict[line[17]], line[4]+'\n']]
                if line[17] in vasopress:
                    write_string +=  [['Vasopressin', 'True', line[4]+'\n']]
                curr_enc = line[1]
                curr_pat = line[0]
    
    
                #except:
                 #   print(line[0]+'-'+curr_enc)
        if curr_enc in enc_list:
            with open('Encounter Files 2\\'+curr_pat+'-'+curr_enc+'.csv', 'a', newline='\n') as f:
                f.writerows(write_string)
            
def max_enc_time(filename):
    '''finds maximum length of encounters'''
    with open('ssdb_encounters\\encounters.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        firstline = next(reader)
        while firstline[3] == 'OUTPATIENT':
            firstline = next(reader)
        
        min_date = time.strptime(firstline[2], '%d%b%Y')
        max_date = time.strptime(firstline[2], '%d%b%Y')
        for line in reader:
            if line[3] != 'OUTPATIENT':
                curr_date = time.strptime(line[2], '%d%b%Y')
                if curr_date < min_date:
                    min_date = curr_date
                if curr_date > max_date:
                    max_date = curr_date
        print(max_date.date())
        print(min_date.date())
                
'''Below is the procedure used to generate the final GMC encounter level data files. If this was repeated some of the functions could be replaced
with their updated versions (eg. filter_vitals with filter_faster)'''                        
#filter_categorical_data('ssdb_encounters.zip', csv_name='encounters.csv', enc=True, enc_inx=0, pat_inx=1)
#filter_categorical_data('ssdb_social_history.zip', csv_name='social_history.csv', enc=True, enc_inx=0, pat_inx=1)
#filter_faster('ssdb_demographics.zip', csv_name='demographics.csv', extra_line = True)
#filter_faster('ssdb_problem_list.zip', csv_name='problem_list.csv')
#filter_categorical_data('ssdb_cohort_admits.zip', csv_name='cohort_admits.csv', enc=True)
#filter_vitals('ssdb_vitals.zip',3,5,4,csv_name='vitals.csv')
#write_2('ssdb_lab_orders.zip', 'lab_orders.csv', 'ssdb_lab_results\\lab_results.csv', 'lab_results_enc.csv')
#filter_vitals_no_zip(2,7,4,'lab_results_sorted.csv')
#print(find_enc_dttm('Encounter Files\\*'))
#print(get_mech_vent('ssdb_billing_procedures\\billing_procedures.csv'))
#print(mv_main('Encounter Files 2\\*', 'ssdb_billing_procedures\\billing_procedures.csv'))
#get_fluids('ssdb_medication_admin\\medication_admin.csv')