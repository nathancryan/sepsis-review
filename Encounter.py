# Alexander Murph
# Encounter objects

import datetime
import time
import csv
from HoCalculator import HoCalculator
from SepsisFinder3 import SepsisFinder
from Fluid_Counter import Fluid_Counter



class Encounter:
	"""
		Abstract for a single patient encounter at a particular hospital.  Provides a way
		to hold data, as well as gives functionality to grabbing particular variables and
		formatting variables into a row in a dataframe.
	"""

	def __init__(self, text_file, hospital_type, range_info):
		self.hospital_type = hospital_type
		self.variable_dict = self.get_var_dict(text_file)
		self.sepsis_time = self.get_sepsis_time()

		shock_time = 'NA'
		if self.get_variable("Fluid Intake") != 'NA' and \
			(self.get_variable("Arterial BP") != 'NA' or self.get_variable("Mean Arterial Pressure") != 'NA') :
			if self.hospital_type == 'GMC':
				shock_time = self.get_HO_shock_time_GMC()
			else:
				shock_time = self.get_HO_shock_time_MIMIC()
		self.shock_time = shock_time

		if range_info[2] == 'Septic Shock':
			end_time = shock_time
			if end_time == 'NA' and self.check_presense_of_variable("DISCH_DT") and self.hospital_type == 'MIMIC':
				end_time =self.determine_value_from_timestamp([self.variable_dict["DISCH_DT"][0][1]])[0]
			elif end_time == 'NA' and self.check_presense_of_variable("ENC_DIS_DTTM") and self.hospital_type == 'GMC':
				end_time = self.determine_value_from_timestamp([self.variable_dict["ENC_DIS_DTTM"][0][0]])[0]
		else:
			end_time = self.get_range_timestamps(range_info[2])
		
		self.start_range = self.get_range_timestamps(range_info[1])
		self.end_range = end_time
		self.number_of_intervals = range_info[0]


	def get_var_dict(self, encounter_text):
		"""
			Takes in a text file for a single encounter, returns a dictionary with the keys
			as the variables names and the entries are the variable values.
		"""
		data_dict = {}
		with open(encounter_text, 'r') as csvfile:
			encounter_reader = csv.reader(csvfile)
			for my_list in encounter_reader:
				if my_list[0] in data_dict.keys():
					data_dict[my_list[0]] += [my_list[1:]]
				else:
					data_dict[my_list[0]] = [my_list[1:]]
		return data_dict

	def get_sepsis_time(self):
		"""
			Takes in the current state of the variable dictionary, searches through
			for certain markers as to the time and date of the onset of sepsis.  In
			this case, we use First BP less than 90 and MAP less than 65.
			If the data is avaliable, it gathers it, if it is not, it simply returns an 'NA'
		"""
		if self.hospital_type == 'GMC':
			return self.get_sepsis_time_from_GMC()
		elif self.hospital_type == 'MIMIC':
			return self.get_sepsis_time_from_MIMIC()			
		else:
			return 'NA'
			

	def get_HO_shock_time_MIMIC(self):
		"""
			Helper function for the get_sepsis_time function.  If there is a value for BP below 90, 
			this function attempts to see if we can make a datetime object out of it and therefore
			use it for our calculations.  If not, we send it off to see if MAP will work.
			This method is specifically for the MIMIC Data.
		"""
		blood_pressure_list = [x[0] for x in self.variable_dict["Arterial BP"]]
		blood_pressure_dates = [x[1] for x in self.variable_dict["Arterial BP"]]
		fluid_intake_list = [x[0] for x in self.variable_dict["Fluid Intake"]]
		fluid_intake_dates = [x[1] for x in self.variable_dict["Fluid Intake"]]

		calculator = HoCalculator(blood_pressure_list, blood_pressure_dates,\
		 fluid_intake_list, fluid_intake_dates)

		date = calculator.get_shock_onset_date()

		if date == 'NA':
			date_complete = date
		else:
			date_complete = datetime.datetime.fromtimestamp(date)
		return date_complete

	def get_HO_shock_time_GMC(self):
		"""
			Helper function for the get_sepsis_time function.  If there is a value for BP below 90, 
			this function attempts to see if we can make a datetime object out of it and therefore
			use it for our calculations.  If not, we send it off to see if MAP will work.
			This method is specifically for the MIMIC Data.
		"""
		blood_pressure_list = [x[0] for x in self.variable_dict["Mean Arterial Pressure"]]
		blood_pressure_dates = [x[1] for x in self.variable_dict["Mean Arterial Pressure"]]
		fluid_intake_list = [x[0] for x in self.variable_dict["Fluid Intake"]]
		fluid_intake_dates = [x[1] for x in self.variable_dict["Fluid Intake"]]

		calculator = HoCalculator(blood_pressure_list, blood_pressure_dates,\
		 fluid_intake_list, fluid_intake_dates)

		date = calculator.get_shock_onset_date()

		if date == 'NA':
			date_complete = date
		else:
			date_complete = datetime.datetime.fromtimestamp(date)
		return date_complete


	def get_sepsis_time_from_GMC(self):
		"""
			Helper function for the get_sepsis_time function.  Gathers the necessary information
			to make a SepsisFinder object that is used to get the best possible time for sepsis
			onset according to the Ho paper.
		"""		
		Temp = 'NA'
		Heart_Rate = 'NA'
		Resp_Rate = 'NA'
		WBC = 'NA'
		BP = 'NA'

		if self.get_variable('Temp') != 'NA':
			Temp = self.variable_dict['Temp']
		if self.get_variable('HR') != 'NA':
			Heart_Rate = self.variable_dict['HR']
		if self.get_variable('Resp') != 'NA':
			Resp_Rate = self.variable_dict['Resp']
		if self.get_variable('WBC') != 'NA':
			WBC = self.variable_dict['WBC']
		if self.get_variable('Arterial BP') != 'NA':
			BP = self.variable_dict['Arterial BP']

		Finder_Object = SepsisFinder(Temp, Heart_Rate, Resp_Rate, WBC, BP, self.hospital_type)

		sepsis_time = Finder_Object.find_sepsis_time()

		return sepsis_time


	def get_sepsis_time_from_MIMIC(self):
		"""
			Helper function for the get_sepsis_time function.  Gathers the necessary information
			to make a SepsisFinder object that is used to get the best possible time for sepsis
			onset according to the Ho paper.
		"""		
		Temp = 'NA'
		Heart_Rate = 'NA'
		Resp_Rate = 'NA'
		WBC = 'NA'
		BP = 'NA'

		if self.get_variable('Temperature F') != 'NA':
			Temp = self.variable_dict['Temperature F']
		if self.get_variable('Heart Rate') != 'NA':
			Heart_Rate = self.variable_dict['Heart Rate']
		if self.get_variable('Respiratory Rate') != 'NA':
			Resp_Rate = self.variable_dict['Respiratory Rate']
		if self.get_variable('WBC') != 'NA':
			WBC = self.variable_dict['WBC']
		if self.get_variable('Arterial BP') != 'NA':
			BP = self.variable_dict['Arterial BP']

		Finder_Object = SepsisFinder(Temp, Heart_Rate, Resp_Rate, WBC, BP, self.hospital_type)

		sepsis_time = Finder_Object.find_sepsis_time()

		return sepsis_time


	def get_variable(self, variable_name, method = "last_value"):
		"""
			Given a variable name, and a method for choosing from many possible values,
			returns the value for this encounter.
			The possible methods are:
				first_value
				last_value
				closest_to_sepsis
				average_value
			If an invalid method is chosen, no method is chosen, or average is chosen and
			the values are not numeric, then last_value is used.
		"""
		if variable_name == "Septic_Date":
			return self.shock_time

		if method == "first_value":
			return self.choose_first_value(variable_name)
		elif method == "value_closest_to_sepsis":
			return self.choose_value_closest_to_sepsis(variable_name)
		elif method == "average_value":
			return self.choose_average_value(variable_name)
		elif method == "value_closest_to_shock":
			return self.choose_value_closest_to_sepsis(variable_name, self.shock_time)
		elif method == "range_of_time":
			return self.choose_value_range(variable_name)
		else:
			return self.choose_last_value(variable_name)


	def get_row(self, variable_list, method = "last_value"):
		"""
			Given a variable list, and a method for choosing from many possible values,
			returns a single row for the creation of a dataframe.
			The possible methods are:
				first_value
				last_value
				closest_to_sepsis
				average_value
			If an invalid method is chosen, no method is chosen, or average is chosen and
			the values are not numeric, then last_value is used.
		"""
		row = []
		for name in variable_list:
			row += [self.get_variable(name, method)]
		return row


	def choose_first_value(self, variable_name):
		"""
			Takes in a variable name, and returns the first value avaliable at that place in the 
			dictionary.
		"""	
		if self.variable_dict[variable_name][0][0] == '' or self.variable_dict[variable_name][0][0] == '\n':
			return 'NA'
		else:
			return self.variable_dict[variable_name][0][0]


	def choose_last_value(self, variable_name):
		"""
			Takes in a variable name, and returns the last value avaliable at that place in the 
			dictionary.
		"""		
		if self.check_presense_of_variable(variable_name):
			wanted_variable = self.variable_dict[variable_name][-1][0]
			if wanted_variable != 'None':
				return wanted_variable
			else:
				return 'NA'
		else:
			return 'NA'	


	def choose_value_closest_to_sepsis(self, variable_name, sep_time = 'None Passed'):
		"""
			Takes in a variable name, and returns the value in the dictionary closest to the time
			the patient got sepsis.
		"""	

		if sep_time == 'None Passed' or sep_time == 'NA':
			sep_time = self.sepsis_time

		if not (self.check_presense_of_variable(variable_name)):
			return 'NA'

		

		if len(self.variable_dict[variable_name][0]) <= 1:
			return self.variable_dict[variable_name][0][0]

		time_stamps = [x[1] for x in self.variable_dict[variable_name]]
			
		if 'None\n' in time_stamps or '\n' in time_stamps or 'None' in time_stamps:
			return self.choose_first_value(variable_name)
		
		time_differences = self.determine_value_from_timestamp(time_stamps)
		kept_indicies = [i for i in range(len(time_stamps)) if (time_differences[i] != 'NA') ]

		if variable_name == 'Fluid Intake' and sep_time != 'NA':
			Water_Bowl = Fluid_Counter(self.variable_dict['Fluid Intake'], time_differences, sep_time, 3600)	
			return Water_Bowl.get_fluids()	

		if variable_name == 'Urine Output' and sep_time != 'NA':
			Pee_Bowl = Fluid_Counter(self.variable_dict['Urine Output'], time_differences, sep_time, 86400)	
			return Pee_Bowl.get_fluids()
		
		
		elif sep_time != 'NA' and sep_time != 'None Passed':

			time_differences = [(sep_time - x) for x in time_stamps if x != 'NA']

			# We edit this line when we wish to only consider values at a certain time.  This became something
			# we did often as we moved on to Markov and clustering work.
			time_differences = [ x.total_seconds() for x in time_differences if (x.total_seconds() >= 0)]
			if time_differences != []:
				ind = self.find_min_index(time_differences)
				return self.variable_dict[variable_name][kept_indicies[ind]][0]
			else:
				return 'NA'
		else:
			return 'NA'

	def get_range_timestamps(self, my_range):
		"""
			A function I had to make because Alex sucks.
		"""

		if not (self.check_presense_of_variable(my_range)):
			return 'NA'

		# This if else statement is because Alex can never do anything right, or consistently.
		
		if my_range == 'ADMIT_DT':
			start_time = self.variable_dict[my_range][0][1]
		else:
			start_time = self.variable_dict[my_range][0][0]
		range_timestamps = self.determine_value_from_timestamp([start_time])
		return range_timestamps[0]


	def choose_value_range(self, variable_name):
		"""
			With this encounter's parameters, finds a list of n values from a discritized series
			of time intervals.
		"""

		if not (self.check_presense_of_variable(variable_name)):
			return 'NA'

		if len(self.variable_dict[variable_name][0]) <= 1:
			return self.variable_dict[variable_name][0][0]

		time_stamps = [x[1] for x in self.variable_dict[variable_name]]

		if 'None\n' in time_stamps or '\n' in time_stamps or 'None' in time_stamps:
			value = self.choose_first_value(variable_name)
			value_list = [value] + ['NA'] * (self.number_of_intervals - 1)
			return value_list

		time_differences = self.determine_value_from_timestamp(time_stamps)
		kept_indicies = [i for i in range(len(time_stamps)) if time_differences[i] != 'NA']

		if not (self.start_range == 'NA' or self.end_range == 'NA'):

			time_differences = [(x - self.end_range) for x in time_stamps if x != 'NA']
			full_range = self.end_range - self.start_range
			full_range = full_range.total_seconds()
			one_interval = full_range / self.number_of_intervals
			value_list = []

			for interval in range(self.number_of_intervals):
				temp_differences = [ x.total_seconds() for x in \
				time_differences if (x.total_seconds() >= (one_interval * interval) and \
				 x.total_seconds() <= (one_interval * (interval + 1))) ]
				if temp_differences != []:
					ind = self.find_min_index(temp_differences)
					value_list += [self.variable_dict[variable_name][kept_indicies[ind]][0]]
				else:
					value_list += ['NA']
			return value_list

		return 'NA'


	def find_min_index(self, differences):
		'''
			Helper function used solely by the choose_value_closest_to_sepsis function.  Takes in a list of
			values, and returns the index of the minimum value.
		'''
		ind = 0
		curr_min = differences[0]
		for index in range(len(differences)):
			if curr_min > differences[index]:
				curr_min = differences[index]
				ind = index
		return ind


	def determine_value_from_timestamp(self, time_stamps):
		"""
			Takes a variable's time stamp and returns a datetime object to be used in calculations in 
			other methods.
		"""
		if self.hospital_type == 'GMC':
			format1 = '%d%b%Y'
			format2 = '%d%b%Y:%H:%M:%S.%f'
			format3 = '%d%b%Y:%H:%M:%S.%f\n'
		else:
			format1 = '%Y-%m-%d'
			format2 = '%Y-%m-%d %H:%M:%S %Z'
			format3 = '%Y-%m-%d %H:%M:%S %Z\n'

		for index in list(range(len(time_stamps))):
			temp_time_string = time_stamps[index]
			try:
				time_stamps[index] = datetime.datetime.strptime(temp_time_string, format2)
			except Exception:
				try:
					time_stamps[index] = datetime.datetime.strptime(temp_time_string, format3)
				except Exception:
					try:
						time_stamps[index] = datetime.datetime.strptime(temp_time_string, format1)
					except Exception:
						time_stamps[index] = 'NA'
		return time_stamps


	def check_presense_of_variable(self, variable_name):
		"""
			Simple method for checking if a variable name is present in our data dictionary.
		"""
		return variable_name in self.variable_dict.keys()

	def choose_average_value(self,variable_name):
		"""
			Takes in a variable name, and returns the average of the values at that place in the 
			dictionary.
		"""		
		values = [x[0] for x in self.variable_dict[variable_name]]
		if all(value.is_integer() for value in values):
			return sum(values)/len(values)
		else:
			return self.choose_last_value(variable_name)


