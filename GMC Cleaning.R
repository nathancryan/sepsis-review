# GMC Cleaning
# Author: Alexander Murph

gmc_data = read.csv('my_data.csv')


# Task 0 - Creat and Fix the classifier
gmc_data$ADMIT_DIAG = ifelse( (as.character(gmc_data$ADMIT_DIAG) == '785.52' | 
                                 as.character(gmc_data$ADMIT_DIAG) == 'R65.21'), TRUE, NA)
gmc_data$PRIDX = ifelse( (as.character(gmc_data$PRIDX) == '785.52' |
                                 as.character(gmc_data$PRIDX) == 'R65.21'), TRUE, NA)
gmc_data$SECDX1 = ifelse( (as.character(gmc_data$SECDX1) == '785.52' | 
                                 as.character(gmc_data$SECDX1) == 'R65.21'), TRUE, NA)
gmc_data$SECDX2 = ifelse( (as.character(gmc_data$SECDX2) == '785.52' | 
                                 as.character(gmc_data$SECDX2) == 'R65.21'), TRUE, NA)
gmc_data$SECDX3 = ifelse( (as.character(gmc_data$SECDX3) == '785.52' | 
                                 as.character(gmc_data$SECDX3) == 'R65.21'), TRUE, NA)
gmc_data$SECDX4 = ifelse( (as.character(gmc_data$SECDX4) == '785.52' | 
                                 as.character(gmc_data$SECDX4) == 'R65.21'), TRUE, NA)
gmc_data$SECDX5 = ifelse( (as.character(gmc_data$SECDX5) == '785.52' | 
                             as.character(gmc_data$SECDX5) == 'R65.21'), TRUE, NA)
gmc_data$SECDX6 = ifelse( (as.character(gmc_data$SECDX6) == '785.52' | 
                             as.character(gmc_data$SECDX6) == 'R65.21'), TRUE, NA)
gmc_data$SECDX7 = ifelse( (as.character(gmc_data$SECDX7) == '785.52' | 
                             as.character(gmc_data$SECDX7) == 'R65.21'), TRUE, NA)
gmc_data$SECDX8 = ifelse( (as.character(gmc_data$SECDX8) == '785.52' | 
                             as.character(gmc_data$SECDX8) == 'R65.21'), TRUE, NA)
gmc_data$SECDX9 = ifelse( (as.character(gmc_data$SECDX9) == '785.52' | 
                             as.character(gmc_data$SECDX9) == 'R65.21'), TRUE, NA)
gmc_data$SECDX10 = ifelse( (as.character(gmc_data$SECDX10) == '785.52' |
                             as.character(gmc_data$SECDX10) == 'R65.21'), TRUE, NA)

# Task 1 - Merge columns that searched multiple places for same variable.
merge_to_variable = function(x,y,z) {
  # Function that takes in three columns of equal length, and reduces them to
  # a single column by useing values from each in ordered priority.
  
  number_of_rows = min(length(x), length(y), length(z))
  new_column = rep(NA, times = number_of_rows)
  for( index in 1:number_of_rows ) {
    if(!is.na(x[index])){
      new_column[index] = x[index]
      next
    } else if(!is.na(y[index])){
      new_column[index] = y[index]
      next      
    } else if(!is.na(z[index])){
      new_column[index] = z[index]
      next      
    }
  }
  return(new_column)
}

gmc_data$Has_Code = merge_to_variable(gmc_data$ADMIT_DIAG, gmc_data$PRIDX, gmc_data$SECDX1)
gmc_data$Has_Code = merge_to_variable(gmc_data$Septic_Date, gmc_data$SECDX2, gmc_data$SECDX3)
gmc_data$Has_Code = merge_to_variable(gmc_data$Septic_Date, gmc_data$SECDX4, gmc_data$SECDX5)
gmc_data$Has_Code = merge_to_variable(gmc_data$Septic_Date, gmc_data$SECDX6, gmc_data$SECDX7)
gmc_data$Has_Code = merge_to_variable(gmc_data$Septic_Date, gmc_data$SECDX8, gmc_data$SECDX9)
gmc_data$Has_Code = merge_to_variable(gmc_data$Septic_Date, gmc_data$SECDX10, gmc_data$SECDX10)

gmc_data$Has_Code = ifelse(is.na(gmc_data$Has_Code), FALSE, TRUE)
gmc_data$Septic_Date = ifelse(is.na(gmc_data$Septic_Date), FALSE, TRUE)

gmc_data$HCO3 = merge_to_variable(gmc_data$BICARBONATE..ART, 
                                  gmc_data$BICARBONATE..VEN, 
                                  gmc_data$BICARBONATE.ISTAT)

gmc_data$PO2 = merge_to_variable(gmc_data$PO2..ARTERIAL, 
                                 gmc_data$PO2..VENOUS, 
                                 gmc_data$PO2.ISTAT)

gmc_data$BILIRUBIN = merge_to_variable(gmc_data$BILIRUBIN..TOTAL, 
                                       gmc_data$BILIRUBIN..TOTAL, 
                                       gmc_data$BILIRUBIN..DIRECT)

gmc_data$FIO2 = merge_to_variable(gmc_data$FiO2, 
                                  gmc_data$FIO2, 
                                  gmc_data$FIO2)

# Task 2 - Reduce to columns needed for models

# Real quick, we fix the outliers in Bilirubin and FIO2, and convert of PT_BIRTH_DT to Age in years.
gmc_data$BILIRUBIN = ifelse(gmc_data$BILIRUBIN > 1000.0, NA, gmc_data$BILIRUBIN)
gmc_data$FIO2 = ifelse(gmc_data$FIO2 > 1000.0, NA, gmc_data$FIO2)
gmc_data$Age = as.POSIXct(gmc_data$PT_BIRTH_DT, format = '%Y-%m-%d', tz = 'EST')
today = as.POSIXct('2016-10-26', format = '%Y-%m-%d', tz = 'EST')
gmc_data$Age = as.numeric(today - gmc_data$Age)/365
gmc_data$Mech.Vent = ifelse(as.character(gmc_data$Mech.Vent) == 'False', FALSE, TRUE) 
gmc_data$Vasopressin = ifelse(is.na(gmc_data$Vasopressin), 0, 1)


wanted_variables = c("Resp","Pulse","BP.Systolic","BP.Diastolic","Temp","Glasgow...Total.Score",
                     "PH..ARTERIAL","SpO2","WBC","PLATELET.COUNT","CREATININE","BUN",
                     "Urine.Output","GLUCOSE","HCT","Age","SODIUM","POTASSIUM",
                     "Mech.Vent","FIO2","Fluid.Intake","HCO3","PO2","BILIRUBIN", 'SODIUM', 'Vasopressin', 'Septic_Date')
gmc_data = gmc_data[wanted_variables]

# Task 3 - Reduce to complete cases
gmc_data_reduced = gmc_data[complete.cases(gmc_data),]

# Task 5 - Calculate SOFA and SAPS
# To get the scores required for each attribute, we will utilize the cut function.
gmc_data_reduced$Age = as.numeric(as.character(cut(gmc_data_reduced$Age, c(0,45,55,65,75,200), 
                           labels = c('0','1','2','3','4'))))

gmc_data_reduced$PulseSOFA = as.numeric(as.character(cut(gmc_data_reduced$Pulse, c(0,39,54,69,109,139,179,250), 
                           labels = c('4','3','2','0','2.0','3.0','4.0'))))

gmc_data_reduced$BP.SystolicSAPS = as.numeric(as.character(cut(gmc_data_reduced$BP.Systolic, c(0,54,79,149,189,300), 
                                                     labels = c('4','2','0','2.0','4.0'))))

gmc_data_reduced$TempSAPS = as.numeric(as.character(cut((as.numeric(as.character(gmc_data_reduced$Temp)) - 32)*(5/9),
                                                        c(0,29.9,31.9,33.9,35.9,38.4,38.9,40.9,60), 
                                                     labels = c('4','3','2','1','0','1.0','3.0','4.0'))))

gmc_data_reduced$RespRate = as.numeric(as.character(cut(gmc_data_reduced$Resp, c(0,5,9,11,24,34,49,100), 
                                                       labels = c('4','2','1','0','1.0','3.0','4.0'))))

gmc_data_reduced$Mech.Vent.SAPS = as.numeric(as.character(cut(as.numeric(gmc_data_reduced$Mech.Vent), c(-1,0.5,2), 
                                                              labels = c('0','3'))))

gmc_data_reduced$BUNSAPS = as.numeric(as.character(cut(gmc_data_reduced$BUN/2.8, c(0,3.4,7.4,28.9,35.9,54.9,100), 
                                                        labels = c('1','0','1.0','2','3','4'))))

gmc_data_reduced$HCT = as.numeric(as.character(cut(gmc_data_reduced$HCT, c(0,19.9,29.9,45.9,49.9,59.9,100), 
                                                       labels = c('4','2','0','1','2.0','4.0'))))

gmc_data_reduced$Urine.Output = as.numeric(as.character(cut(as.numeric(gmc_data_reduced$Urine.Output)/1000.0,
                                                            c(0,0.199,0.499,0.699,3.499,4.999,20), 
                                                        labels = c('4','3','2','0','1.0','2.0'))))

gmc_data_reduced$WBCSAPS = as.numeric(as.character(cut(gmc_data_reduced$WBC, c(0,0.9,2.9,14.9,19.9,39.9,200), 
                                                     labels = c('4','2','0','1','2.0','4.0'))))

gmc_data_reduced$GLUCOSE = as.numeric(as.character(cut(gmc_data_reduced$GLUCOSE/18, c(0,3.4,7.4,28.9,35.9,54.9,100), 
                                                       labels = c('1','0','1.0','2','3','4'))))

gmc_data_reduced$POTASSIUM = as.numeric(as.character(cut(gmc_data_reduced$POTASSIUM,
                                                         c(0,2.4,2.9,3.4,5.4,5.9,6.9,20), 
                                                     labels = c('4','2','1','0','2.0','3','4.0'))))

gmc_data_reduced$SODIUM = as.numeric(as.character(cut(gmc_data_reduced$SODIUM,
                                                         c(0,109,119,129,150,155,160,179,200), 
                                                         labels = c('4','3','2','0','1','2.0','3.0','4.0'))))

gmc_data_reduced$HCO3 = as.numeric(as.character(cut(gmc_data_reduced$HCO3,
                                                         c(0,4.9,9.9,19.9,29.9,39.9,100), 
                                                         labels = c('4','3','1','0','1.0','4.0'))))

gmc_data_reduced$Glasgow.Total.ScoreSAPS = as.numeric(as.character(cut(gmc_data_reduced$Glasgow...Total.Score, 
                                                                     c(0,3,6,9,12,16), 
                                                                     labels = c('4','3','2', '1', '0'))))

gmc_data_reduced$SAPS = gmc_data_reduced$Age + gmc_data_reduced$PulseSOFA + gmc_data_reduced$BP.SystolicSAPS +
  gmc_data_reduced$TempSAPS + gmc_data_reduced$RespRate + gmc_data_reduced$Mech.Vent.SAPS +
  gmc_data_reduced$BUNSAPS + gmc_data_reduced$HCT + gmc_data_reduced$Urine.Output + gmc_data_reduced$WBCSAPS +
  gmc_data_reduced$GLUCOSE + gmc_data_reduced$POTASSIUM + gmc_data_reduced$SODIUM + gmc_data_reduced$HCO3 + 
  gmc_data_reduced$Glasgow.Total.ScoreSAPS


gmc_data_reduced$Glasgow.Total.ScoreSOFA = as.numeric(as.character(cut(gmc_data_reduced$Glasgow...Total.Score, 
                                                                       c(0,5,9,12,14, 300), 
                                                                       labels = c('4','3','2', '1', '0'))))

gmc_data_reduced$PLATLET.COUNT = as.numeric(as.character(cut(gmc_data_reduced$PLATELET.COUNT, 
                                                             c(0,19,49,99,159, 10000), 
                                                             labels = c('4','3','2', '1', '0'))))

gmc_data_reduced$BILIRUBINSOFA = as.numeric(as.character(cut(gmc_data_reduced$BILIRUBIN, c(0, 1.1,1.9,5.9,11.9, 3000), 
                                                             labels = c('0','1','2', '3', '4'))))


gmc_data_reduced$CREATININE = as.numeric(as.character(cut(gmc_data_reduced$CREATININE, c(0, 1.1,1.9,3.4,4.9, 3000), 
                                                             labels = c('0','1','2', '3', '4'))))

gmc_data_reduced$PAO2FIO2 = as.numeric(as.character(cut((gmc_data_reduced$PO2 / gmc_data_reduced$FIO2)*100,
                                                        c(0, 99,219,299,399, 3000), 
                                                        labels = c('4','3','2', '1', '0'))))

gmc_data_reduced$SOFA = gmc_data_reduced$Glasgow.Total.ScoreSOFA + gmc_data_reduced$BILIRUBINSOFA +
                            gmc_data_reduced$CREATININE + gmc_data_reduced$PLATLET.COUNT +
                            gmc_data_reduced$PAO2FIO2 + gmc_data_reduced$Vasopressin

gmc_data_reduced = gmc_data_reduced[complete.cases(gmc_data_reduced),]

# Task 6 - Prepare data for modeling phase

zero_one_scale = function(x){
  # Function that takes in a vector of float-like values and returns
  # a scaled version of said vector.  This function assumes a lack of
  # NA values.
  # Author: Alexander Murph
  if(!is.numeric(x)) {
    return(x)
  }
  return( (x - min(x)) / (max(x) - min(x)) )
}

wanted_variables = c("Pulse","Resp","SOFA","SpO2","WBC","SAPS","Temp","BP.Systolic",
                     "BP.Diastolic",'Septic_Date')

gmc_data_final = gmc_data_reduced[wanted_variables]
gmc_data_final$Temp = as.numeric(gmc_data_final$Temp)

gmc_data_final = sapply(gmc_data_final, zero_one_scale)

write.csv(gmc_data_final, 'GMC_Complete_Data_Unreduced.csv')
