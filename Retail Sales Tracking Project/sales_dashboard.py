import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt
from sklearn.linear_model import LinearRegression
import numpy as np

# Snowflake connection with error handling (hardcoded credentials for local testing)
try:
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database='RETAIL_DB',
        schema='SALES'
    )
except snowflake.connector.errors.InterfaceError as e:
    st.error(f"Failed to connect to Snowflake: InterfaceError - {str(e)}")
    st.error("Possible causes: Invalid credentials, incorrect account name, or network issues.")
    st.stop()
except snowflake.connector.errors.DatabaseError as e:
    st.error(f"Database error: {str(e)}")
    st.error("Possible causes: Database, schema, or warehouse does not exist, or user lacks permissions.")
    st.stop()
except Exception as e:
    st.error(f"Unexpected error during connection: {str(e)}")
    st.stop()

# Cache queries for 1 hour to optimize performance
@st.cache_data(ttl=3600)
def fetch_data(query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        df.columns = [col.lower() for col in df.columns]
        return df
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"Query failed: {str(e)}")
        st.error(f"Failed query: {query}")
        st.stop()
    except Exception as e:
        st.error(f"Unexpected error during query execution: {str(e)}")
        st.stop()

# Define SQL queries for all analyses (adjusted for available data)
queries = {
    "revenue_by_product": """
        SELECT product_name, SUM(quantity * price) AS total_revenue
        FROM sales_data
        GROUP BY product_name
    """,
    "sales_over_time": """
        SELECT DATE_TRUNC('MONTH', sale_date) AS month, SUM(quantity * price) AS total_revenue
        FROM sales_data
        GROUP BY month
        ORDER BY month
    """,
    "quantity_by_product": """
        SELECT product_name, SUM(quantity) AS total_quantity
        FROM sales_data
        GROUP BY product_name
        ORDER BY total_quantity DESC
    """,
    "sales_by_day": """
        SELECT DAYNAME(sale_date) AS day_of_week, SUM(quantity * price) AS total_revenue
        FROM sales_data
        GROUP BY day_of_week
        ORDER BY total_revenue DESC
    """,
    "slow_moving": """
        SELECT product_name, SUM(quantity) AS total_quantity
        FROM sales_data
        GROUP BY product_name
        ORDER BY total_quantity ASC
        LIMIT 5
    """,
    "historical_sales": """
        SELECT DATE_TRUNC('MONTH', sale_date) AS month, product_name, SUM(quantity * price) AS total_revenue
        FROM sales_data
        GROUP BY month, product_name
        ORDER BY month, product_name
    """,
    "price_vs_quantity": """
        SELECT product_name, AVG(price) AS avg_price, SUM(quantity) AS total_quantity
        FROM sales_data
        GROUP BY product_name
    """,
    "product_by_month": """
        SELECT DATE_TRUNC('MONTH', sale_date) AS month, product_name, SUM(quantity * price) AS total_revenue
        FROM sales_data
        GROUP BY month, product_name
        ORDER BY month, product_name
    """,
    "sample_data": """
        SELECT * FROM sales_data LIMIT 5
    """
}

# Fetch data for all queries
try:
    data = {key: fetch_data(query) for key, query in queries.items()}
    # Ensure 'month' columns are Timestamps
    for key in ["sales_over_time", "product_by_month", "historical_sales"]:
        if "month" in data[key].columns:
            data[key]["month"] = pd.to_datetime(data[key]["month"])
except Exception as e:
    st.error(f"Failed to fetch data: {str(e)}")
    st.stop()

# Convert numerical columns to float
for key in ["revenue_by_product", "sales_over_time", "sales_by_day", "historical_sales", "price_vs_quantity", "product_by_month"]:
    if "total_revenue" in data[key].columns:
        data[key]["total_revenue"] = data[key]["total_revenue"].astype(float)
    if "avg_price" in data[key].columns:
        data[key]["avg_price"] = data[key]["avg_price"].astype(float)
    if "total_quantity" in data[key].columns:
        data[key]["total_quantity"] = data[key]["total_quantity"].astype(float)

# Streamlit dashboard
st.title("Decoding Digital Age: E-Commerce Sales Insights Pipeline")

# Sidebar for filtering
st.sidebar.header("Filters")
selected_product = st.sidebar.selectbox("Select Product", options=["All"] + sorted(data["revenue_by_product"]["product_name"].unique()))
date_range = st.sidebar.date_input("Select Date Range", value=(pd.to_datetime("2025-01-01"), pd.to_datetime("2025-03-31")))

# Filter data based on selections
filtered_revenue = data["revenue_by_product"]
filtered_quantity = data["quantity_by_product"]
filtered_price_quantity = data["price_vs_quantity"]
filtered_product_month = data["product_by_month"]
if selected_product != "All":
    filtered_revenue = filtered_revenue[filtered_revenue["product_name"] == selected_product]
    filtered_quantity = filtered_quantity[filtered_quantity["product_name"] == selected_product]
    filtered_price_quantity = filtered_price_quantity[filtered_price_quantity["product_name"] == selected_product]
    filtered_product_month = filtered_product_month[filtered_product_month["product_name"] == selected_product]

# Apply date range filter
start_date, end_date = date_range
start_date = pd.to_datetime(start_date)  # Ensure Timestamp
end_date = pd.to_datetime(end_date)      # Ensure Timestamp
for key in ["sales_over_time", "product_by_month"]:
    data[key]["month"] = pd.to_datetime(data[key]["month"])  # Ensure Timestamp
    data[key] = data[key][(data[key]["month"] >= start_date) & (data[key]["month"] <= end_date)]
if selected_product != "All":
    filtered_product_month["month"] = pd.to_datetime(filtered_product_month["month"])
    filtered_product_month = filtered_product_month[
        (filtered_product_month["month"] >= start_date) & 
        (filtered_product_month["month"] <= end_date)
    ]

# Revenue by Product
st.subheader("Revenue by Product")
chart = alt.Chart(filtered_revenue).mark_bar().encode(
    x=alt.X('total_revenue:Q', title='Total Revenue ($)', axis=alt.Axis(format='$,.0f')),
    y=alt.Y('product_name:N', title='Product Name', sort='-x'),
    tooltip=['product_name', 'total_revenue']
).properties(width=600, height=400, title='Revenue by Product')
st.altair_chart(chart, use_container_width=True)

# Sales Over Time
st.subheader("Sales Trends Over Time")
chart = alt.Chart(data["sales_over_time"]).mark_line().encode(
    x=alt.X('month:T', title='Month'),
    y=alt.Y('total_revenue:Q', title='Total Revenue ($)', axis=alt.Axis(format='$,.0f')),
    tooltip=['month', 'total_revenue']
).properties(width=600, height=400, title='Sales Over Time')
st.altair_chart(chart, use_container_width=True)

# Quantity by Product
st.subheader("Quantity Sold by Product")
chart = alt.Chart(filtered_quantity).mark_bar().encode(
    x=alt.X('total_quantity:Q', title='Total Quantity Sold'),
    y=alt.Y('product_name:N', title='Product Name', sort='-x'),
    tooltip=['product_name', 'total_quantity']
).properties(width=600, height=400, title='Quantity Sold by Product')
st.altair_chart(chart, use_container_width=True)

# Sales by Day of the Week
st.subheader("Sales by Day of the Week")
chart = alt.Chart(data["sales_by_day"]).mark_bar().encode(
    x=alt.X('total_revenue:Q', title='Total Revenue ($)', axis=alt.Axis(format='$,.0f')),
    y=alt.Y('day_of_week:N', title='Day of Week', sort='-x'),
    tooltip=['day_of_week', 'total_revenue']
).properties(width=600, height=400, title='Sales by Day of the Week')
st.altair_chart(chart, use_container_width=True)

# Slow-Moving Inventory
st.subheader("Slow-Moving Inventory (Bottom 5)")
st.write(data["slow_moving"])

# Predicted Sales
st.subheader("Predicted Sales for Next Month")
def predict_future_sales(df_historical):
    predictions = []
    for product in df_historical["product_name"].unique():
        product_data = df_historical[df_historical["product_name"] == product].copy()
        if len(product_data) < 2:  # Need at least 2 months for prediction
            continue
        product_data["month_num"] = np.arange(len(product_data))
        X = product_data[["month_num"]]
        y = product_data["total_revenue"]
        model = LinearRegression()
        model.fit(X, y)
        next_month = len(product_data)
        predicted_revenue = model.predict([[next_month]])[0]
        predictions.append({"product_name": product, "predicted_revenue": predicted_revenue})
    return pd.DataFrame(predictions)

historical_sales = data["historical_sales"]
predictions = predict_future_sales(historical_sales)
st.write(predictions)

# Price vs. Quantity Analysis
st.subheader("Price vs. Quantity Analysis")
chart = alt.Chart(filtered_price_quantity).mark_circle(size=100).encode(
    x=alt.X('avg_price:Q', title='Average Price ($)'),
    y=alt.Y('total_quantity:Q', title='Total Quantity Sold'),
    color='product_name:N',
    tooltip=['product_name', 'avg_price', 'total_quantity']
).properties(width=600, height=400, title='Price vs. Quantity Sold')
st.altair_chart(chart, use_container_width=True)

# Product Performance by Month
st.subheader("Product Performance by Month")
chart = alt.Chart(filtered_product_month).mark_bar().encode(
    x=alt.X('month:T', title='Month'),
    y=alt.Y('total_revenue:Q', title='Total Revenue ($)', axis=alt.Axis(format='$,.0f'), stack=True),
    color='product_name:N',
    tooltip=['month', 'product_name', 'total_revenue']
).properties(width=600, height=400, title='Product Performance by Month')
st.altair_chart(chart, use_container_width=True)

# Display raw data
st.subheader("Sample Data")
st.write(data["sample_data"])

# Close the connection
conn.close()
