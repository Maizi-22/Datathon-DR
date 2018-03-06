###### Logistic regression for sepsis project 
###### And use logistic regression to predict hospital motality/ICU motality
###### Input: Data, outcome variable, fluid variable
###### Output: model summary(a table contained OR, confidence level and p value); ROC of prediction
RegressionAndPredict <- function(train, test, y){
  # logistic regression 
  train.data <- train
  test.data <- test
  
  # apply linear regression
  data <- train.data
  # model1 = glm(data[[y]] ~ gender + los_hos + los_icu + add_drug_therapy + weight_adm + bbr + acei + mra + atrial_fibrilation + hyperlipidemia + renal_failure + nosocomial_hypoproteinemia + nosocomial_anemia + nosocomial_hyponatremia + daily_fluid_overload_to_rrt + urine_output_to_rrt + daily_uo_to_rrt + total_output_to_discharge + fluid_overload_to_dis + urine_output_to_dis + hr_therapy + sbp_therapy + dbp_adm + bun_dr + potassium_therapy + hemoglobin_adm + ph_dis
  #              , family = binomial(link='logit')
  #              , data = data)
  model1 = glm(data[[y]] ~ age + gender + add_drug_therapy + change_drug_therapy + height + weight_adm + weight_after_dr + 
weight_before_dialysis + weight_gain_before_rrt_perday + bbr + inotropes_vasopressor + acei + mra + vasodilator + 
ischemic_heart_disease + cardiomyopathies + valvular_disease + atrial_fibrilation + hyperlipidemia + 
hypertension + diabetes + sleep_disordered_breathing + renal_failure + anemia + infection + alcohol_abuse + 
daily_fluid_overload_to_rrt + daily_uo_to_rrt + hr_adm + hr_dr + sbp_adm + sbp_dr + dbp_adm + dbp_dr + 
spo2_adm + spo2_dr + tem_adm + tem_dr + base_excess_adm + bun_adm + bun_dr + creatinine_adm + 
creatinine_dr + sodium_adm + sodium_dr + potassium_adm + potassium_dr + hemoglobin_adm + hemoglobin_dr + hco3_adm + 
hco3_dr + gfr_adm + gfr_dr + ph_adm + ph_dr + albumin_adm + albumin_dr
               , family = binomial(link='logit')
               , data = data)
  model1.summary <- summary(model1)
  model2 <- step(model1)
  
  # test on test data
  data <- test.data
  model2 <- glm(model2$formula
                , family = binomial(link='logit')
                , data = data)
  
  # AUC of ROC curve
  test.data$pred2 <-predict(model2, type='response', data = test.data)
  rocobj1 <- plot.roc(as.matrix(test.data[, y]),
                      test.data$pred2,
                      percent=TRUE,ci=TRUE,col="#1c61b6",
                      print.auc=TRUE)
  result <- rocobj1
  return(result)
}



# data interpolation ------------------------------------------------------
# input : dataset
# output : data frame

DataInterp <- function(dataset){
  # start with lab test and vital test!
  mydata <- dataset
  mydata <- mydata[which(mydata$hadm_id != 100557), ] # this patient have many missing values
  mydata <- mydata[which(mydata$hadm_id != 102966), ] # this patient may have wrong weight values
  mydata <- mydata[which(mydata$hadm_id != 116431), ] # this patient may have wrong weight values
  mydata <- mydata[which(mydata$hadm_id != 142440), ] # this patient may have wrong weight values
  mydata <- mydata[which(mydata$hadm_id != 153971), ] # this patient may have wrong weight values
  # mydata <- mydata[which(mydata$hadm_id != 175544), ] # this patient may have wrong weight values after interpolation
  # mydata <- mydata[which(mydata$hadm_id != 148373), ] # this patient may have wrong fluid values after interpolation
  # mydata <- mydata[which(mydata$hadm_id != 117044), ] # this patient may have wrong hemo values after interpolation
  # # remove four cases that contain too many missings
  # mydata <- mydata[!is.na(mydata$sbp_adm), ]
  
  # use muti interpolation to handle missing lab test data on therapy and dr date
  library(mice)
  MiData <- function(data, vary, var1, var2, var3){
    # the interpolation would act on rrt and no-rrt patients seperately
    imp <- mice(data[which(data$rrt == 1),][c(vary, var1, var2, var3)], seed=1234, method = 'norm')
    hadm_id <- data[which(data$rrt == 1), c('hadm_id')]
    a1 <- cbind(hadm_id, complete(imp,action=3))
    
    imp2 <- mice(data[which(data$rrt == 0),][c(vary, var1, var2, var3)], seed=1234, method = 'norm')
    hadm_id <- data[which(data$rrt == 0), c('hadm_id')]
    a2 <- cbind(hadm_id, complete(imp2,action=3))
    
    a <- rbind(a1, a2)
    return(a)
  }
  # for vital test data 
  hr.mi <- MiData(mydata, "hr_therapy", "hr_adm", "hr_dr", "hr_dis")
  sbp.mi <- MiData(mydata, "sbp_therapy", "sbp_adm", "sbp_dr", "sbp_dis")
  dbp.mi <- MiData(mydata, "dbp_therapy", "dbp_adm", "dbp_dr", "dbp_dis")
  spo2.mi <- MiData(mydata, "spo2_therapy", "spo2_adm", "spo2_dr", "spo2_dis")
  spo2.mi$spo2_therapy[spo2.mi$spo2_therapy > 100] <- 100
  spo2.mi$spo2_dr[spo2.mi$spo2_dr > 100] <- 100
  tem.mi <- MiData(mydata, "tem_therapy", "tem_adm", "tem_dr", "tem_dis")
  tem.mi$tem_therapy[tem.mi$tem_therapy == 0] <- median(tem.mi$tem_therapy)
  vital.mi <- merge(hr.mi, sbp.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  vital.mi <- merge(vital.mi, dbp.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  vital.mi <- merge(vital.mi, spo2.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  vital.mi <- merge(vital.mi, tem.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  
  # for lab test data
  ph.mi <- MiData(mydata, "ph_therapy", "ph_adm", "ph_dr", "ph_dis")
  albumin.mi <- MiData(mydata, "albumin_therapy", "albumin_adm", "albumin_dr", "albumin_dis")
  gfr.mi <- MiData(mydata, "gfr_therapy", "gfr_adm", "gfr_dr", "gfr_dis")
  gfr.mi$gfr_therapy[gfr.mi$gfr_therapy < 0] <- median(mydata$gfr_therapy, na.rm = T)
  hco3.mi <- MiData(mydata, "hco3_therapy", "hco3_adm", "hco3_dr", "hco3_dis")
  hemo.mi <- MiData(mydata, "hemoglobin_therapy", "hemoglobin_adm", "hemoglobin_dr", "hemoglobin_dis")
  pota.mi <- MiData(mydata, "potassium_therapy", "potassium_adm", "potassium_dr", "potassium_dis")
  sod.mi <- MiData(mydata, "sodium_therapy", "sodium_adm", "sodium_dr", "sodium_dis")
  crea.mi <- MiData(mydata, "creatinine_therapy", "creatinine_adm", "creatinine_dr", "creatinine_dis")
  crea.mi$creatinine_therapy[crea.mi$creatinine_therapy < 0] <- median(mydata$creatinine_therapy, na.rm = T)
  bun.mi <- MiData(mydata, "bun_therapy", "bun_adm", "bun_dr", "bun_dis")
  bun.mi$bun_therapy[bun.mi$bun_therapy < 0] <- median(mydata$bun_therapy, na.rm = T)
  # base excess only have value on admission, so we examine correlation between be and orther variables
  # finally use hco3 and ph to compute base excess where it's missing
  # correlation <- cor(lab, lab[, "base_excess_adm"], use="pairwise.complete.obs")   
  # correlation <- abs(correlation)
  # correlation
  be.mi <- MiData(mydata, "base_excess_adm", "hco3_adm", "ph_adm", "hco3_dr")[, c(1,2)]
  # merge lab test data
  lab.mi <- merge(ph.mi, albumin.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(gfr.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(hco3.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(hemo.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(pota.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(sod.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(crea.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(bun.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  lab.mi <- merge(be.mi, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = T)
  
  # handle fluid load data 
  # patient whose hadm_id = 148373 may have wrong records of fluid input and fluid load
  mydata[which(mydata[, "hadm_id"] == '148373'), c("total_input_to_discharge", "fluid_overload_to_dis", "fluid_overload_to_rrt", "daily_fluid_overload_to_rrt")] <- NA
  # find out that 143 patients don't have fluid records, so here we assume these patients's fluid records are 0
  # It's not suitable to interpulate this data, consider discard imcomplete cases
  fluid.mi <- mydata[, c('hadm_id', "fluid_overload_to_rrt", "daily_fluid_overload_to_rrt", 'urine_output_to_rrt'
                         , 'daily_uo_to_rrt', 'total_output_to_discharge', "total_input_to_discharge", "fluid_overload_to_dis"
                         , 'urine_output_to_dis')][complete.cases(mydata[, c('hadm_id', "fluid_overload_to_rrt"
                                                                               , "daily_fluid_overload_to_rrt", 'urine_output_to_rrt', 'daily_uo_to_rrt', 'total_output_to_discharge', "total_input_to_discharge", "fluid_overload_to_dis", 'urine_output_to_dis')]), ]
  # when merge with basic table, set na to 0
  
  # handle with basic table
  basic.mi1 <- mice(mydata[which(mydata$rrt == 1),][c(2:8, 10:18)], seed=1234, method = 'norm')
  a1 <- cbind(mydata[which(mydata$rrt == 1), c('hadm_id')], complete(basic.mi1,action=3))
  names(a1)[1] <- 'hadm_id'
  basic.mi2 <- mice(mydata[which(mydata$rrt == 0),][c(2:8, 10:18)], seed=1234, method = 'norm')
  a2 <- cbind(mydata[which(mydata$rrt == 0), c('hadm_id')], complete(basic.mi2,action=3))
  names(a2)[1] <- 'hadm_id'
  basic.mi <- rbind(a1, a2)
  
  data.f <- merge(mydata[, c(1, 2)], basic.mi[, -2], by.x = 'hadm_id', by.y = 'hadm_id', all.x = TRUE)
  data.f <- merge(data.f, mydata[, c(1, 19:35)], by.x = 'hadm_id', by.y = 'hadm_id', all.x = TRUE)
  # data.f <- merge(data.f, mydata[, c(1, 46:48)], by.x = 'hadm_id', by.y = 'hadm_id', all.x = TRUE)
  data.f[is.na(data.f)] <- 0
  data.f <- merge(data.f, fluid.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = TRUE)
  data.f <- merge(data.f, vital.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = TRUE)
  data.f <- merge(data.f, lab.mi, by.x = 'hadm_id', by.y = 'hadm_id', all.x = TRUE)
  data.f <- merge(data.f, mydata[, c("hadm_id", 'rrt')], all.x = TRUE)
  data.f <- data.f[complete.cases(data.f), ]
  # don't know how to do with two variables: diure_resis_to_rrt_day, diure_drug_use_to_rrt_day, ingore them for now
  # summary(mydata)
  # return(data.f)
}

