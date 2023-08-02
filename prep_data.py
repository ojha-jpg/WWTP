import calendar
import pandas as pd 
import os
column_names=pd.read_csv("columns.csv")
folder_path="Monthlydata_excel"
import numpy as np

dfs=[]
print("---------------STARTED---------------")
for file_name in os.listdir(folder_path):
    if file_name.endswith('.xlsx') or file_name.endswith('.xls'):  # Check if the file is an Excel file``
        file_path = os.path.join(folder_path, file_name)
        print(file_path)
        data = pd.read_excel(file_path,skiprows=7,usecols=range(103))
        data.columns=column_names['Names']
        data["Date"] = pd.to_datetime(data["Date"],errors='coerce') 
        first_date = data['Date'].iloc[4]
        month = first_date.month
        days_in_month = calendar.monthrange(first_date.year, month)[1]        
        data =data.iloc[:days_in_month]        
        dfs.append(data)
        
                 
combined_df = pd.concat(dfs, ignore_index=True)
combined_df = combined_df.drop_duplicates(subset=['Date'])
combined_df = combined_df.drop(['Date1', 'Date2','Date3','Date4','Date5','Date6'], axis=1)
combined_df=combined_df.sort_values(by="Date")
combined_df.set_index('Date', inplace=True)
for column in combined_df.columns:
    combined_df[column] = pd.to_numeric(combined_df[column], errors='coerce')


# converting empty and zeros into \N so that sql reads it as NULL
combined_df =combined_df.replace (r'^\s*$',np.nan,regex=True)
combined_df = combined_df.replace(np.nan,'\\N', regex=True)
combined_df = combined_df.replace(0.00,'\\N', regex=True)



combined_df.to_csv("Processed_data/data.csv")
print("----COMPLETED---------")
    

        
        