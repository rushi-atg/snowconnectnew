import requests
import json
import pandas as pd
import tabulate
import logging  # Used to display messages instead of jusy printing them
import snowflake.connector
import helper
from pandas import json_normalize

config = helper.read_config()
user = config['snow_logging']['user']
password = config['snow_logging']['password']
account = config['snow_logging']['account']
region = config['snow_logging']['region']
warehouse = config['snow_logging']['warehouse']
database = config['snow_logging']['database']
schema = config['snow_logging']['schema']

con_eb = snowflake.connector.connect(user=user,
                                     password=password,
                                     account=account,
                                     region=region,
                                     warehouse=warehouse,
                                     database=database,
                                     schema=schema,
                                     autocommit=True)

db_cursor_eb = con_eb.cursor()

# CREATES TABLE TRANSACTIONS
db_cursor_eb.execute("""CREATE TABLE new_data.Blockchain_stats if not exists(
                        datav variant);"""
                     )

#https://randomuser.me/api/
response_API = requests.get('https://api.blockchain.info/stats')
print(response_API.status_code)
dictr = response_API.json()
print (dictr['timestamp'],dictr['market_price_usd'])
qw=dictr['market_price_usd']

# INSERTS DATA INTO TRANSACTIONS TABLE
db_cursor_eb.execute("insert into Blockchain_stats (select PARSE_JSON('%s'))"% json.dumps(dictr))












