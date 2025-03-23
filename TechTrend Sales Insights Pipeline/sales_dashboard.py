import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt

conn = snowflake.connector.connect(
    user = 'kothiyal',
    password = 'Kothiyal@642912',
    account = 'WSPPEEP-XN49709',
    warehouse = 'COMPUTE_WH',
    database = 'RETAIL_DB',
    schema = 'SALES'
)

# Query data
revenue_query = """
SELECT product_name, SUM(quantity * price) AS total_revenue
FROM sales_data
GROUP BY product_name
"""
cursor = conn.cursor()
cursor.execute(revenue_query)
df_revenue = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

# Debug: Print the DataFrame to check column names
st.write("Debug: DataFrame columns:", df_revenue.columns.tolist())
st.write("Debug: DataFrame preview:", df_revenue.head())

# Rename columns to lowercase
df_revenue.columns = [col.lower() for col in df_revenue.columns]

# Convert total_revenue to float to avoid Altair warning
df_revenue['total_revenue'] = df_revenue['total_revenue'].astype(float)

# Streamlit dashboard
st.title("TechTrend Sales Dashboard")
st.subheader("Revenue by Product")

# Create a custom Altair chart
chart = alt.Chart(df_revenue).mark_bar().encode(
    x=alt.X('total_revenue:Q', title='Total Revenue ($)', axis=alt.Axis(format='$,.0f')),
    y=alt.Y('product_name:N', title='Product Name', sort='-x'),
    tooltip=['product_name', 'total_revenue']
).properties(
    width=600,
    height=400,
    title='Revenue by Product'
)

# Display the chart
st.altair_chart(chart, use_container_width=True)

# Display raw data
st.subheader("Sample Data")
cursor.execute("SELECT * FROM sales_data LIMIT 5")
df_sample = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
df_sample.columns = [col.lower() for col in df_sample.columns]
st.write(df_sample)

# Close the connection
conn.close()