
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

# Create table
cursor.execute("""
    CREATE OR REPLACE TABLE happiness (
        "Country name" STRING,
        "Ladder score" FLOAT,
        "Logged GDP per capita" FLOAT
    )
""")
print("Table created.")

# Load data from the stage with explicit column mapping
cursor.execute("""
    COPY INTO happiness ("Country name", "Ladder score", "Logged GDP per capita")
    FROM (SELECT $1, $3, $7 FROM @DEMO_DB.PUBLIC.HAPPINESS_STAGE/world_happiness_2021.csv)
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE'
""")
print("Data loaded into table.")

# Verify the number of rows loaded
cursor.execute("SELECT COUNT(*) FROM happiness")
row_count = cursor.fetchone()[0]
print(f"Number of rows in table: {row_count}")

# Query the top 5 happiest countries
if row_count > 0:
    cursor.execute("""
        SELECT "Country name", "Ladder score"
        FROM happiness
        ORDER BY "Ladder score" DESC
        LIMIT 5
    """)
    top_5 = cursor.fetchall()
    print("Top 5 happiest countries:")
    for row in top_5:
        print(f"{row[0]}: {row[1]}")

    # Query average GDP per capita
    cursor.execute('SELECT AVG("Logged GDP per capita") FROM happiness')
    avg_gdp = cursor.fetchone()[0]
    print(f"Average GDP per capita: {avg_gdp}")
else:
    print("No data loaded into the table. Check the COPY INTO step.")

# Close the connection
conn.close()