# Fluid Counter Object
# Author: Alexander Murph
# Fluid_Counter(self.variable_dict['Fluid Intake'], time_stamps, sep_time) <- Call

import datetime
import time

class Fluid_Counter:

	def __init__(self, var_dict, time_stamps, sep_time, amount):
		"""
			Given a length of range that we are interested in (amount), gather all fluid values found within that range
			of sepsis time and add them all up.
		"""
		just_values = [x[0] for x in var_dict]
		self.values = [just_values[i] for i in range(len(just_values)) if time_stamps[i] != 'NA']
		self.time_stamps = time_stamps
		self.sep_time = sep_time
		self.amount = amount

	def get_fluids(self):
		"""
			A helper function for the fluid counter object.
		"""
		time_differences = [(x - self.sep_time) for x in self.time_stamps if x != 'NA']
		relevant_fluids = [float(self.values[i]) for i in range(len(self.time_stamps)) if time_differences[i].total_seconds() <= self.amount]
		if relevant_fluids == []:
			return 'NA'
		return sum(relevant_fluids)




