# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 12:58:06 2016

@author: Alex
"""

'''code to collate downloaded MIMIC II files into a directory filled with a single file for each patient. Each patient file if formatted as a csv
file with three columns: variable name, variable value, and timestamp. MIMIC source directory must be formatted as follows: main folder\\first two digits of patient id #s
\\patient directories beginning with ##\\patient files'''


import glob
import csv

def admit_dis(filename):
    '''gathers information from ADMISSIONS'''
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        firstline = next(reader)
        info = next(reader)
        write_string = []
        write_string += [[firstline[2],'NA', info[2], '\n']]
        write_string += [[firstline[3],'NA', info[3], '\n']]
    return write_string
def septic_shock(filename):
    '''tests whether or not a patient has the icd9 diagnosis for septic shock'''
    with open(filename, 'r') as f:
        reader=csv.reader(f)

        shock = False
        for line in reader:
            if line[3] == "785.52":
                shock = True
        if shock == True:
            write_string = ['Septic Shock', 'True', '\n']
        else:
            write_string = ['Septic Shock', 'False', '\n']
    return write_string

def get_fluid_list(filename):
    '''extracts fluid names from fluid_list.csv'''
    fluid_list = set()
    with open(filename, 'r') as f:
        reader= csv.reader(f)
        for line in reader:
            fluid_list.add(line[0])
    return fluid_list
    

def make_chart_dict(filename):
    '''helper function that relates chart item id #s to english names'''
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        chart_dict = {}
        for line in reader:
            chart_dict[line[0]] = line[1]
            
    return chart_dict
 
def io_events(filename,fluid_list, subject_id):
    '''adds information from IOEVENTS (needs fluid_list)'''
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        write_string = []
        try:
            first_line = next(reader)
            for line in reader:
                if line[2] in fluid_list:
                    #print('done')
                    if line[9] != '':
                        item_name = 'Fluid Intake'
                
                
                        item_val = line[9]
                        item_time = line[3]
                        write_string += [[item_name, item_val, item_time+'\n']]
                        
        except StopIteration:
            with open('error_file.txt', 'a') as ef:
                ef.write(subject_id+ ' proc')
    return write_string
    
   
def get_demographics(filename):
    '''adds info from D_patients'''
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        write_string = []
        try:
            first_line = next(reader)
            data = next(reader)
            subject_id = data[0]
            for i in range(len(first_line)):
                write_string += [[first_line[i], data[i], 'None\n']]
            f.seek(0)
        except StopIteration:
            with open('error_file.txt', 'a') as ef:
                ef.write(subject_id + ' demo')
            
    return subject_id, write_string
        
def procedure_events(filename, subject_id):
    '''adds info from POE_EVENTS'''
    
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        write_string = []
        try:
            first_line = next(reader)
            for line in reader:
                
                item_name = line[7]
                
                item_val = line[18]+' '+line[19]
                item_time = line[4]
                write_string += [[item_name, item_val, item_time+'\n']]
            f.seek(0)
        except StopIteration:
            with open('error_file.txt', 'a') as ef:
                ef.write(subject_id+ ' proc')
                
    return write_string
    
def chart_items(filename, chart_dict, subject_id):
    '''adds info from CHARTEVENTS'''
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        write_string = []
        try:
            first_line = next(reader)
            for line in reader:
                
                item_name = chart_dict[line[2]]
                
                if line[9] == '':
                    item_val = line[8]
                else:
                    item_val = line[9]
                if line[12] == '':
                    
                    item_val2 = line[11]
                else:
                    item_val2 = line[12]
                item_time = line[5]
                if item_val2 == '':
                    write_string += [[item_name, item_val, item_time+'\n']]
                else:
                    write_string += [[item_name, item_val, item_time+'\n'],[item_name+'val_2', item_val2, item_time+'\n']]
            f.seek(0)
        except StopIteration:
            with open('error_file.txt', 'a') as ef:
                ef.write(subject_id+ ' chart')
                
    return write_string
    
def main_writer(start_path, destination):
    '''runs through ## level directories (see top) and creates patient level csvs for each patient in the directory. Adds data from the 
    CHARTEVENTS, POE_ORDER, ICD9, ADMISSIONS, and D_PATIENTS files for each patient. Can also add data from IOEVENTS but needs list of desired
    fluid names in file called fluids_list.csv'''
    chart_dict = make_chart_dict('D_CHARTITEMS.txt')
    for pat_dir in glob.glob(start_path):
        demo_file = glob.glob(pat_dir+'\\D_PATIENTS-*')
        #fluid_list = get_fluid_list('fluids.csv')
        
        pat_id, demo_data = get_demographics(demo_file[0])
        chart_file = glob.glob(pat_dir+'\\CHARTEVENTS-*')
        proc_file = glob.glob(pat_dir+'\\POE_ORDER-*')
        icd9_file = glob.glob(pat_dir+'\\ICD9-*')
        #io_file = glob.glob(pat_dir+'\\IOEVENTS-*')
        adm_file = glob.glob(pat_dir+'\\ADMISSIONS-*')
        shock = septic_shock(icd9_file[0])
        write_string = chart_items(chart_file[0], chart_dict, pat_id)
        proc_string = procedure_events(proc_file[0], pat_id)
        #io_string = io_events(io_file[0], fluid_list, pat_id)

        adm_string = admit_dis(adm_file[0])

        with open(destination+'mimic-'+pat_id+'.csv', 'w', newline='\n') as wf:
             writer = csv.writer(wf)
             writer.writerows(demo_data)
             writer.writerows(write_string)
             #writer.writerows(io_string)
             writer.writerows(proc_string)
             writer.writerow(shock)
             writer.writerows(adm_string)

def main_runner(top_path, destination):
    '''main function that outputs a single directory full of collated patient files. Input: top_path: the top of the MIMIC file path as a string.
    format is name\\*. destination: directory name for output as string. Format is name\\.'''
    for start_path in glob.glob(top_path):
        main_writer(start_path+'\\*', destination)
