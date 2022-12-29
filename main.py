import logging  # Used to display messages instead of jusy printing them
import snowflake.connector
import helper

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
db_cursor_eb.execute("""CREATE TABLE new_data.TRANSACTIONS if not exists(
                        TRX_ID BIGINT PRIMARY KEY,
                        SOURCE_CURRENCY_CODE VARCHAR(3),
                        TARGET_CURRENCY_CODE VARCHAR(3),
                        USER_TYPE VARCHAR(20),
                        USER_COUNTRY VARCHAR(100),
                        AMOUNT_GBP FLOAT);"""
                     )

# When successful, displays message 'Table TRANSACTIONS successfully created'
logging.info(db_cursor_eb.fetchone()[0])

# INSERTS DATA INTO TRANSACTIONS TABLE
db_cursor_eb.execute("""
                INSERT INTO new_data.TRANSACTIONS (TRX_ID, SOURCE_CURRENCY_CODE, TARGET_CURRENCY_CODE,
                                                      USER_TYPE, USER_COUNTRY, AMOUNT_GBP)
                values(1232,'dfg','kyu','ete','reter',198.3);"""
                     )
# Displays number of rows inserted,
logging.info('Total rows inserted: %s', db_cursor_eb.rowcount)

