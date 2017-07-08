# Cleaning and preparing MIMIC 2 data
# Author: Alexander Murph

rm(list = ls())

MIMIC_data = read.csv('MIMIC_ClosestToShock.csv')

MIMIC_data$Septic.Shock = ifelse(is.na(MIMIC_data$Septic_Date), FALSE, TRUE)
MIMIC_data = MIMIC_data[-10]


full_MIMIC_data = MIMIC_data[complete.cases(MIMIC_data),]

# Next, we check that all our data types are what we want.  Indeed,
sapply(full_MIMIC_data, class)

full_MIMIC_data$WBC = as.numeric(full_MIMIC_data$WBC)

zero_one_scale = function(x){
  # Function that takes in a vector of float-like values and returns
  # a scaled version of said vector.  This function assumes a lack of
  # NA values.
  # Author: Alexander Murph
  return( (x - min(x)) / (max(x) - min(x)) )
}

scaled_MIMIC_data = data.frame(cbind(sapply(full_MIMIC_data[,-10], zero_one_scale),
                          Septic_Date = as.logical(full_MIMIC_data$Septic.Shock)))

nrow(scaled_MIMIC_data)
write.csv(scaled_MIMIC_data, 'MIMIC_Complete_Data.csv')

