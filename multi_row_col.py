import requests
import json
import tabulate
import logging  # Used to display messages instead of jusy printing them
import snowflake.connector
from snowflake.sqlalchemy import URL
import helper
from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
from pandas import json_normalize

config = helper.read_config()
user = config['snow_logging']['user']
password = config['snow_logging']['password']
account = config['snow_logging']['account']
region = config['snow_logging']['region']
warehouse = config['snow_logging']['warehouse']
database = config['snow_logging']['database']
schema = config['snow_logging']['schema']

engine = create_engine(URL(user=user, password=password,account=account, region=region,warehouse=warehouse, database=database, schema=schema, autocommit=True))


response_API = requests.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population')
print(response_API.status_code)
dictr = response_API.json()
print (dictr)
if_exists = 'replace'
table='population_multi'
table1='population_multi1'
#dict = json.loads(response_API)
df2 = json_normalize(dictr['data'])
df2['Day'] = "Monday"
#df['Day'] = ['Monday', 'Tuesday', 'Wednesday', 'Thursday']
#df.insert(loc=1, column="Stars", value=[2,2,3,4])
#df['Month'] = {'Jan':'Foreign Cinema', 'Feb':'Liho Liho', 'Apr':'500 Club', 'Dec':'Square'}
print(df2)

# Create a DataFrame
#df = pd.DataFrame(df2, columns=['ID Nation', 'Nation','ID Year','Year','Population','Slug Nation','city'])
# Write the data from the DataFrame to the table named "population_multi".
df2.to_sql(table, engine, index=False,if_exists=if_exists, method=pd_writer)
#success = write_pandas(con_eb, df2, table1)


