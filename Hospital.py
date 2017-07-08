# Alexander Murph
# Hospital Objects

import dill

from Encounter import *
import csv
from os import listdir
from os.path import isfile, join
import pickle
import sys


class Hospital:
	"""
		Abstract for a group of patients.  Takes in a directory and formats each file into seperate 
		patient encounters.
	"""

	def __init__(self, directory, hospital_type, range_info = ['NA', 'NA', 'NA']):
		self.hospital_type = hospital_type
		self.range_info = range_info
		self.directory = self.get_encounters(directory)
		


	def get_encounters(self, text_directory):
		""" 
			Method for creating a list of encounter objects from a directory of encounter text files.
		"""
		encounter_list = []
		counter = 0
		onlyfiles = [f for f in listdir(text_directory) if f[0] != '.']
		for encounter_text in onlyfiles:
			counter += 1
			if counter >= 120 and counter < 200:
				print(counter)
				encounter_text = text_directory + '/' + encounter_text
				new_encounter = Encounter(encounter_text, self.hospital_type, self.range_info)
				encounter_list += [new_encounter]
		return encounter_list


	def get_Dataframe(self, variable_list, method_type = "last_value"):
		"""
			Method that takes in a list of variables, and returns a dataframe in the form of a 
			matrix that uses all of our encounters and gives those variable values.
		"""
		data_matrix = []
		for encounter_object in self.directory:
			single_row = encounter_object.get_row(variable_list, method_type)
			data_matrix += [single_row]
		return data_matrix


	def write_file(self, filename, variable_list, method_type = "last_value"):
		"""
			Method that takes in a list of variables, creates a dataframe using the encounters
			and those variables, then writes that dataframe to a csv file.
		"""		
		with open(filename, 'w') as mycsvfile:
			the_data_writer = csv.writer(mycsvfile)
			array_of_data = self.get_Dataframe(variable_list, method_type)
			the_data_writer.writerow(variable_list)
			for row in array_of_data:
				the_data_writer.writerow(row)


hospital1 = Hospital('Mimic_patient_files', 'MIMIC')

hospital2 = Hospital('GMC_Patient_Files', 'GMC')


hospital1.write_file("my_data.csv", ['PT_ID', 'ENC_ID', 'Resp', 'Pulse', 'BP Systolic', 'BP Diastolic', 'Temp', 'Glasgow - Total Score', \
	'PH, ARTERIAL', 'SpO2', 'WBC', 'PLATELET COUNT', 'CREATININE', 'BUN', 'Urine Output', 'FiO2', 'GLUCOSE', 'HCT', \
	'PT_BIRTH_DT', 'SODIUM', 'POTASSIUM', 'Mech Vent', 'BILIRUBIN, TOTAL', 'BILIRUBIN, UA', 'BILIRUBIN, DIRECT', 'BICARBONATE, ART', \
	'FIO2', 'PO2, ARTERIAL', 'Fluid Intake', 'PO2, VENOUS', 'PO2 ISTAT', 'BICARBONATE, VEN', 'BICARBONATE ISTAT', 'Septic_Date', \
	'Vasopressin', 'SODIUM', 'ADMIT_DIAG', 'PRIDX', 'SECDX1', 'SECDX2', 'SECDX3', 'SECDX4', 'SECDX5', 'SECDX6', 'SECDX7', 'SECDX8', \
	'SECDX9', 'SECDX10'], "value_closest_to_shock")

hospital2.write_file("GMC_Range_Start_Shock.csv", ['Resp', 'Pulse', 'BP Systolic', 'BP Diastolic', 'Temp', \
 'SpO2', 'WBC', 'Septic_Date'], "value_closest_to_shock")

