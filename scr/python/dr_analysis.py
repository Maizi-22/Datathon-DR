import pandas as pd
import numpy as np
import os.path as op
path = '~/Documents/python-projects/Datathon-DR/data/'
basic_info = pd.read_csv(op.join(path,'basic_info.csv'))
drug = pd.read_csv(op.join(path,'druguse.csv'))
fluidload = pd.read_csv(op.join(path,'fluidload.csv'))
nosocomial = pd.read_csv(op.join(path,'nosocomial.csv'))
vital = pd.read_csv(op.join(path,'vital.csv'))

basic_info = basic_info.drop(['subject_id'], axis=1)
drug = drug.drop(['subject_id'],axis=1)
fluidload = fluidload.drop(['subject_id'], axis = 1)
nosocomial = nosocomial.drop(['subject_id'], axis=1)
vital = vital.drop(['subject_id'], axis=1)


rawdata = basic_info.merge(drug, on = 'hadm_id', how = 'left')
rawdata = rawdata.merge(fluidload, on = 'hadm_id', how = 'left')
rawdata = rawdata.merge(nosocomial, on = 'hadm_id', how = 'left')
rawdata = rawdata.merge(vital, on = 'hadm_id', how = 'left')

rawdata