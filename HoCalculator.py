# Author: Alexander Murph
# Ho Calculator Object

import datetime
import time

class HoCalculator:

	def __init__(self, pressure_list, pressure_date, fluid_list, fluid_date):
		"""
			Abstract for the calculation done to find variables in the Ho paper done
			in the literature review.  Views areas of hypotension and converts them
			into ranges.  If the fluid intake an hour before the range to the center of
			the range is greater than 600ml, the beginning of the range is the time from
			which the variables are collected.
		"""
		self.pressure_list = pressure_list
		self.fluid_list = fluid_list
		self.pressure_date = self.format_dates(pressure_date, False)
		self.fluid_date = self.format_dates(fluid_date)

	def format_dates(self, date_list, fluid_output = True):
		"""
			Private utility function the converts every date object into the desired format.
			In the case where we don't have the data to create a date object, the value at that
			index and that value at the index of the value list are both removed.
		"""	

		format1 = '%Y-%m-%d %H:%M:%S %Z\n'
		format2 = '%d%b%Y:%H:%M:%S.%f\n'
		format3 = '%d%b%Y:%H:%M:%S.%f'
		indicies_to_remove = []
		for index in range(len(date_list)):
			caught_exception = False
			try:
				new_date = datetime.datetime.strptime(date_list[index], format1)
			except Exception:
				try:
					new_date = datetime.datetime.strptime(date_list[index], format2)
				except Exception:
					try:
						new_date = datetime.datetime.strptime(date_list[index], format3)
					except Exception:
						indicies_to_remove += [index]
						caught_exception = True
			if not caught_exception:
				date_list[index] = new_date

		date_list = self.remove_value_at_indicies(indicies_to_remove, date_list, fluid_output)
		return date_list


	def remove_value_at_indicies(self, indicies, value_list, fluid_output):
		"""
			Private helper function used by the format_dates function.  Edits our instance
			variables in the case that we need certain values removed.
		"""		
		if fluid_output:
			dates_without = [value_list[i] for i in range(len(value_list)) if i not in indicies]
			values_without = [self.fluid_list[i] for i in range(len(self.fluid_list)) if i not in indicies]
			self.fluid_list = values_without
			return dates_without
		else:
			dates_without = [value_list[i] for i in range(len(value_list)) if i not in indicies]
			values_without = [self.pressure_list[i] for i in range(len(self.pressure_list)) if i not in indicies]
			self.pressure_list = values_without
			return dates_without


	def get_shock_onset_date(self):
		"""
			Only function to be called by the object.  Every other method should be 
			considered private.  This function uses the instance variables and the
			other functions to return the ordinal datetime of septic shock onset.
		"""
		runs_of_hypotension = self.get_hypotension_runs()
		runs_of_hypotension_dates = self.get_hypotension_date_ranges(runs_of_hypotension)
		list_of_onset_dates = self.get_onset_dates(runs_of_hypotension_dates)
		if list_of_onset_dates == []:
			return 'NA'
		else:
			return min(list_of_onset_dates)

	def get_hypotension_runs(self):
		"""
			Private function that looks at all ranges of hypotension and collects runs.
			That is, it collects consecutive dates if each date is a period of hypotension.
		"""
		run_list_part = []
		run_list_whole = []
		for i in range(len(self.pressure_list)):
			if self.pressure_list[i] == '':
				self.pressure_list[i] = 100
			if float(self.pressure_list[i]) < 90:
				run_list_part += [i]
			else:
				if run_list_part != []:
					run_list_whole = [run_list_part]
					run_list_part = []
		return run_list_whole

	def get_hypotension_date_ranges(self, run_values):
		"""
			Private function that takes the list of runs, converts the dates into ordinal, 
			and creates a list for each run showing the time an hour before the start,
			the time that it starts, and the time that it at the center of the range.
		"""
		run_ranges = []
		for index_list in run_values:
			dates = [self.pressure_date[index] for index in index_list]
			dates = [time.mktime(x.timetuple()) for x in dates]
			hypotension_start = min(dates)
			hour_before_start = min(dates) - 3600
			center_of_range = sum(dates)/len(dates)
			date_range = [hour_before_start, hypotension_start, center_of_range]
			run_ranges += [date_range]
		return run_ranges

	def get_onset_dates(self, date_ranges):
		"""
			Private function that takes the organized date ranges and checks to see if
			there was enough fluid intake in the range to count the date as a possible
			time of septic shock onset.
		"""
		onset_dates = []
		dates = self.fluid_date
		dates = [time.mktime(x.timetuple()) for x in dates]
		for range_of_dates in date_ranges:
			indices = [i for i in range(len(dates)) if \
			 (self.is_date_valid(dates[i], range_of_dates[0], range_of_dates[2])) ]
			values = [float(self.fluid_list[index]) for index in indices]
			if sum(values) > 600:
				onset_dates += [range_of_dates[1]]
		return onset_dates


	def is_date_valid(self, date, hour_before_date, center_of_range_date):
		"""
			Private helper function that checks to see if a given date is within the range
			of hypotension.
		"""
		return ( (date >= hour_before_date) & (date <= center_of_range_date))
			

