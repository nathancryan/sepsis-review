# Alexander Murph
# SepsisFinder object

import datetime
import time

DELTA = 1800

class SepsisFinder:

	def __init__(self, Temp, Heart_Rate, Resp_Rate, WBC, BP, hospital_type):
		'''
			Goes through, in order, the possible markers for a sepsis diagnosis by the definition
			provided in the Ho paper.  The markers are:
				Temperature F < 36 or >38
				Heart Rate > 90
				Respiratory Rate > 20
				WBC > 12 or < 4

			Author: Alexander Murph
		'''

		self.Temp = Temp
		self.Heart_Rate = Heart_Rate
		self.Resp_Rate = Resp_Rate
		self.WBC = WBC
		self.BP = BP
		self.hospital_type = hospital_type


	def find_sepsis_time(self):
		'''
			Function to be called on the object to begin the process of finding the necessary
			sepsis time.
		'''
		Temp_date = self.get_time_from_variable(self.Temp, 96.8, 100.4)
		HR_date = self.get_time_from_variable(self.Heart_Rate, -1, 90)
		Resp_date = self.get_time_from_variable(self.Resp_Rate, -1, 20)
		WBC_date = self.get_time_from_variable(self.WBC, 4, 12)
		BP_date = self.get_time_from_variable(self.BP, -1, 20)

		Temp_date = [x for x in Temp_date if x != 'NA']
		HR_date = [x for x in HR_date if x != 'NA']
		Resp_date = [x for x in Resp_date if x != 'NA']
		WBC_date = [x for x in WBC_date if x != 'NA']
		BP_date = [x for x in BP_date if x != 'NA']

		sepsis_count = 0
		indicies = []

		potential_dates = Temp_date + HR_date + Resp_date + WBC_date + BP_date
		index_values = [1] * len(Temp_date) + [2] * len(HR_date) + [3] * len(Resp_date) + \
						[4] * len(WBC_date) + [5] * len(BP_date) 
		ordered_dates = self.order_timestamps([potential_dates, index_values])
		if ordered_dates == 'NA':
			return 'NA'

		return self.find_first_sepsis(ordered_dates)

	def find_first_sepsis(self, ordered_dates):
		'''
			Takes in ordered date time objects and the indicator indicides and returns the earliest instance
			of two happening within a half an hour of each other.
		'''
		dates = ordered_dates[::2]
		indicies = ordered_dates[1::2]

		for index in range(len(dates)):
			for inner_index in range(index, len(dates)):
				if ( (dates[index] - dates[inner_index]).total_seconds() + DELTA ) > 0:
					if indicies[inner_index] != indicies[index]:
						return dates[index]
				else:
					break
		return 'NA'



	def order_timestamps(self, dates_and_indicies):
		'''
			Recursive function that takes in all relevent time stamps and an index list keeping track of which
			timestamps came from which variable, and returns the time stamps in order.
		'''
		if len(dates_and_indicies[0]) == 0:
			return 'NA'
		elif len(dates_and_indicies[0]) == 1:
			return [dates_and_indicies[0][0], dates_and_indicies[1][0]]
		else:
			min_index = self.find_min_value(dates_and_indicies)
			date = dates_and_indicies[0][min_index]
			index = dates_and_indicies[1][min_index]

			truncated_dates = dates_and_indicies[0][0:min_index] + dates_and_indicies[0][(min_index + 1):]
			truncated_indicies = dates_and_indicies[1][0:min_index] + dates_and_indicies[1][(min_index + 1):]

			return  [date, index] + self.order_timestamps([truncated_dates, truncated_indicies]) 


	def find_min_value(self, dates_and_indicies):
		'''
			Takes in a list of dates, returns the index at which the earliest date occured 
			and its corresponding value from the index list.  
		'''
		min_index = 0

		for index in range(len(dates_and_indicies[0])):
			if( dates_and_indicies[0][index] - dates_and_indicies[0][min_index] ).total_seconds() < 0:
				min_index = index

		return min_index


	def get_time_from_variable(self, variable_list, range_low, range_high):
		"""
			Helper function for the find_sepsis_time function.  Checks to see if this encounter
			satisfies the given conditions for the given variables.  If not, returns an NA, if so,
			returns a date.time object.
		"""

		if variable_list == 'NA':
			return ['NA']

		value_list = [x[0] for x in variable_list]
		wanted_value_index = []
		for value_index in range(len(value_list)):
			if value_list[value_index] == '' or \
			value_list[value_index] in 'NA' or \
			'c' in value_list[value_index] or "C" in value_list[value_index] or \
			'F' in value_list[value_index] or "f" in value_list[value_index] or \
			"n" in value_list[value_index] or "*" in value_list[value_index] or \
			"<" in value_list[value_index]:
				continue
			elif float(value_list[value_index]) < range_low or float(value_list[value_index]) > range_high:
				wanted_value_index += [value_index]
				break

		if wanted_value_index == []:
			return ['NA']

		dates = self.determine_value_from_timestamp( [variable_list[x][1] for x in wanted_value_index] )
		complete_dates = [x for x in dates if x != ['NA']]

		return complete_dates

	def determine_value_from_timestamp(self, time_stamps):
		"""
			Takes a variable's time stamp and returns a datetime object to be used in calculations in 
			other methods.

			Method taken from Encounter object.
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



