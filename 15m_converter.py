
import os, sys
from multiprocess import Pool, cpu_count


path = 'Z:/MARKETDATA/DS/IQFEED/1MIN/STK/'
logs = r'Z:/MARKETDATA/Converted_data/STK/Logs/log_15_minconverter.txt'
outpath = r'Z:/MARKETDATA/Converted_data/STK/15m/'
file_extension = '.csv'
file_list = [f for f in os.listdir(path) if f.endswith(file_extension)]

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


def resample_csv(pars):
   import pandas as pd
   import numpy as np
   
   def datetime_index_raw(df):
      df['datetime'] = df['date'] + ' ' + df['time']
      df['datetime'] = pd.to_datetime(df['datetime'], format = '%d.%m.%Y %H%M%S')
      df = df.set_index(df['datetime'])
      del df['date']
      del df['time']
      del df['datetime']
      return df
      

   file, outpath, path = pars
   
   header =  ['date', 'time', 'open', 'high', 'low', 'close','vol0','vol1']
   df = pd.read_csv(path+file, sep = ',', header=None, names = header, dtype = {0:str,1:str})
   df = datetime_index_raw(df)
   df = df[['open','high','low','close','vol0','vol1']]

   df = df.resample('15T').agg({'open': 'first', 
                         'high':'max', 
                         'low':'min', 
                         'close':'last',
                         'vol0':'sum',
                         'vol1':'sum'})
   df['vol0'].replace(0, np.nan, inplace=True)
   df['vol1'].replace(0, np.nan, inplace=True)
   df.dropna(how='all', inplace=True)
   
   df.to_csv(outpath+file)



if __name__ == "__main__":
   log_writer(4)
   try:
       
      pars_list = [[file, outpath, path] for file in file_list]
      #print (pars_list.index)
      workers = 4#int(cpu_count() / 2)
      
      with Pool(processes=workers) as pool:
          pool.map(resample_csv, pars_list)
          log_writer(1)
          
          
   except KeyboardInterrupt:
      pool.terminate()
      pool.join()
      log_writer(2)


