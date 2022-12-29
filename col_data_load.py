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
db_cursor_eb.execute("""CREATE TABLE new_data.Blockchain_stats1 if not exists(
                        n_tx BIGINT,
                        n_blocks_mined BIGINT);"""
                     )

#https://randomuser.me/api/
response_API = requests.get('https://api.blockchain.info/stats')
print(response_API.status_code)
dictr = response_API.json()
print (dictr['timestamp'],dictr['market_price_usd'])
qw=dictr['n_tx']
we=dictr['n_blocks_mined']

# INSERTS DATA INTO TRANSACTIONS TABLE
add_bit = ("INSERT INTO new_data.Blockchain_stats1 "
              "(n_tx, n_blocks_mined) "
              "VALUES (%(n_tx)s, %(n_blocks_mined)s)")

data_bit = dictr

db_cursor_eb.execute(add_bit, data_bit)












