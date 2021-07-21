get_ipython().system('pip install glob')
get_ipython().system('pip install pandas')
get_ipython().system('pip install requests')
get_ipython().system('pip install datetime')


# Imports

import glob
import pandas as pd
from datetime import datetime


# download dataset  

get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json')
get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json')
get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv')


#Extract json

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


# Extract Function

def extract():
    path = 'bank_market_cap_1.json'
    json_df = extract_from_json(path)
    return json_df
        
extracted_data = extract()

csv_path = 'exchange_rates.csv'
csv_dataframe = pd.read_csv(csv_path, index_col=0)
exchange_rate = csv_dataframe.loc['GBP']['Rates']

# Transform
# - Changes the `Market Cap (US$ Billion)` column from USD to GBP
# - Rounds the Market Cap (US$ Billion)` column to 3 decimal places
# - Rename `Market Cap (US$ Billion)` to `Market Cap (GBP$ Billion)`

def transform(dataframe, exchange_rate):
    dataframe['Market Cap (US$ Billion)'] = round(dataframe['Market Cap (US$ Billion)'] * exchange_rate, 3)
    dataframe.rename(columns={"Market Cap (US$ Billion)": "Market Cap (GBP$ Billion)"}, inplace=True)
    return dataframe


# Load

def load(dataframe):
    dataframe.to_csv('bank_market_cap_gbp.csv', index=False)


# Logging Function

def log(message):
    time_format = '%Y-%m-%d-%H:%M:%S'
    timestamp = datetime.now()
    date_formatted = timestamp.strftime(time_format)
    
    with open('logs.txt', 'a') as f:
        f.write(f'{date_formatted} {message}')


# Running the ETL Process
log('ETL Job Started')
log('Extract phase Started')
df_extracted = extract()
log('Extraxt phase Ended')


log('Transform phase Started')
df_transformed = transform(df_extracted, exchange_rate)
log('Transform phase Ended')

log('Load phase Started')
load(df_transformed)
log('Load phase Ended')
