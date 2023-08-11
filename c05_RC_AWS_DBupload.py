#!/usr/bin/python
# -*- coding: utf-8 -*-
# version 1.0.0

# This code uploads data from Russel Creeck weather stations stored in *.csv files to 
# corresponding  'raw_S**' SQL tables at the "viuhydro_wx_data_v2" database on galiano server
# modified by S. Marchenko from code by J. Bodart

import os
import csv
import pandas as pd 
import numpy as np
import os.path
from sqlalchemy import create_engine


# Read csv file with login details and generate a connection with MySQL database
config_data = {}
with open('Z:/FLNRO/Russell Creek/Data/DB/code_2_db/config.csv', newline='') as csvfile:
    config = csv.reader(csvfile, delimiter=',')
    for row in config:
        config_data[row[0]] = row[1]
url = ('mysql+mysqlconnector://' + config_data['config_username']  + ':' +
                                   config_data['config_password']  + '@' +
                                   config_data['config_host']      + ':' +
                                   config_data['config_port']      + '/' +
                                   config_data['config_database'])
engine = create_engine(url, echo = False)
del csvfile, config, row, config_data, url

# source folder
os.chdir ("Z:/FLNRO/Russell Creek/Data/DB/data_2_db")

# source and target names of files/tables
namesin  = ['S1.txt', 'S2.txt', 'S4.txt', 'S6.txt', 'S7.txt', 'S8.txt', 'S9.txt', 'S10.txt']
namesout = ['raw_steph1', 'raw_steph2', 'raw_steph4', 'raw_steph6', 'raw_steph7', 'raw_steph8', 'raw_steph9', 'raw_steph10']


for ind in range(len(namesin)):
    # Read in data from a CSV file
    with open(namesin[ind], 'r') as readFile:
        datain = pd.read_csv(readFile,low_memory=False)
        datain = datain[2::] # get rid of first two rows
        datain = datain.reset_index(drop=True) # reset index to 0
    
    # create the datetime column
    tau = datain.tmp2 + '-' + datain.tmp3 + '-' + datain.tmp4 + ' ' + datain.tmp5 + ':' + datain.tmp6 + ':' + datain.tmp7
    
    # data to be exported to the SQL table
    dataout = pd.DataFrame({
           'DateTime'       :tau,
           'WatYr'          :datain["tmp8"].astype(int),
           'Air_Temp'       :datain["tmp11"].astype(float),
           'RH'             :datain["tmp12"].astype(float),
           'BP'             :datain["tmp10"].astype(float),
           'Wind_speed'     :datain["tmp13"].astype(float)*3.6,
           'Wind_Dir'       :datain["tmp15"].astype(float),
           'Pk_Wind_Speed'  :datain["tmp14"].astype(float)*3.6,
           'Pk_Wind_Dir'    :np.nan,
           'PC_Tipper'      :np.nan,
           'PP_Tipper'      :datain["tmp18"].astype(float),
           'PC_Raw_Pipe'    :datain["tmp22"].astype(float)*1000,
           'PP_Pipe'        :np.nan,
           'Snow_Depth'     :datain["tmp23"].astype(float),
           'SWE'            :np.nan,
           'Solar_Rad'      :datain["tmp24"].astype(float),
           'SWU'            :np.nan,
           'SWL'            :np.nan,
           'LWU'            :np.nan,
           'LWL'            :np.nan,
           'Lysimeter'      :np.nan,
           'Soil_Moisture'  :np.nan,
           'Soil_Temperature':np.nan,
           'Batt'           :datain["tmp9"].astype(float),
           })
    
    dataout.Air_Temp      = dataout.Air_Temp     .round(decimals = 2)
    dataout.RH            = dataout.RH           .round(decimals = 2)
    dataout.BP            = dataout.BP           .round(decimals = 2)
    dataout.Wind_speed    = dataout.Wind_speed   .round(decimals = 2)
    dataout.Wind_Dir      = dataout.Wind_Dir     .round(decimals = 2)
    dataout.Pk_Wind_Speed = dataout.Pk_Wind_Speed.round(decimals = 2)
    dataout.PP_Tipper     = dataout.PP_Tipper    .round(decimals = 2)
    dataout.PC_Raw_Pipe   = dataout.PC_Raw_Pipe  .round(decimals = 1)
    dataout.Snow_Depth    = dataout.Snow_Depth   .round(decimals = 3)
    dataout.Solar_Rad     = dataout.Solar_Rad    .round(decimals = 2)
    dataout.Batt          = dataout.Batt         .round(decimals = 2)
    
    
    # to save to MySQL database
    dataout.to_sql(name=namesout[ind], con=engine, if_exists = 'replace', index=False)

