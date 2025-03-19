
import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='kothiyal',
    password='',
    account='WSPPEEP-XN49709',
    warehouse='COMPUTE_WH',
    database='DEMO_DB',
    schema='PUBLIC'
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
db_schema = cursor.fetchone()
print(f"Connected to database: {db_schema[0]}, schema: {db_schema[1]}")
conn.close()

