# -*- coding: utf-8 -*-


#!/usr/bin/python
import os, sys
import pandas as pd
import shutil
import os
path = 'Z:/MARKETDATA/DS/IQFEED/1MIN/STK/'
logs = r'Z:/MARKETDATA/Converted_data/STK/Logs/log_EOD_minconverter.txt'
outpath = r'Z:/MARKETDATA/Converted_data/STK/EOD/'

def log_writer(inputs):
    #1 = succesfully
    #2 = Errors
    #3 = UN-succesfully
    #4 = started

    from datetime import datetime
    File_object = open(logs,"a+")
    date_time = (datetime.now())
    date_time = str(date_time)
    if inputs == 1:
        print("Market data succesfully converted")
        print(datetime.now())
        L = [date_time ,'   ', "Market data succesfully converted \n"]
        File_object.writelines(L)
        File_object.close() 
    elif inputs == 2:
        print("Caught KeyboardInterrupt, terminating workers")
        print(datetime.now())
        L = [date_time ,'   ', "Caught KeyboardInterrupt, terminating workers \n"]
        File_object.writelines(L)
        File_object.close() 
    elif inputs == 3:
        print("Market data NOT converted")
        print(datetime.now())
        L = [date_time ,'   ', "Market data NOT converted \n"]
        File_object.writelines(L)
        File_object.close() 
    elif inputs == 4:
        print("Market data converter started")
        print(datetime.now())
        L = [date_time ,'   ', "Market data converter started \n"]
        File_object.writelines(L)
        File_object.close() 

file_extension = '.csv'
file_list = [f for f in os.listdir(path) if f.endswith(file_extension)]
def datetime_index_raw(df):
   df['time'] = df['time'].apply(str)
   df.loc[df[df['time']=='0'].index,'time'] = '000000'
   df['datetime'] = df['date'] + ' ' + df['time']
   df['datetime'] = pd.to_datetime(df['datetime'], format = '%d.%m.%Y %H%M%S')
   df = df.set_index(pd.DatetimeIndex(df['datetime']))
   del df['date']
   del df['time']
   del df['datetime']
   return df
log_writer(4)
for file in file_list:
   print(file)
   header =  ['date', 'time', 'open', 'high', 'low', 'close','vol0','vol1']
   df = pd.read_csv(path+file, sep = ',', header=None, names = header)
   df = datetime_index_raw(df)
   df = df['close']
   df = df.resample('D').last()
   df = df.dropna(axis=0,how='all')
   df.to_csv(outpath+file)


log_writer(1)