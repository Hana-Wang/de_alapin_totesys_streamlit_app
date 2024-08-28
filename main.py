from pg8000.native import Connection
from dotenv import load_dotenv
import os

import streamlit as st
import tableauserverclient as TSC

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_views_dataframe

import pandas as pd
import matplotlib.pyplot as plt

load_dotenv()

# print(st.secrets)

def get_connection():
    try:
        # First try to load from Streamlit Secrets (for production)
        return Connection(
            user=st.secrets["database"]["POSTGRES_USERNAME"],
            password=st.secrets["database"]["POSTGRES_PASSWORD"],
            database=st.secrets["database"]["POSTGRES_DATABASE"],
            host=st.secrets["database"]["POSTGRES_HOSTNAME"],
            port=int(st.secrets["database"]["POSTGRES_PORT"]),
        )
    except Exception:
        # Fallback to local environment variables (for local development)
        return Connection(
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DATABASE"),
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=int(os.getenv("POSTGRES_PORT")),
        )



# def get_connection():
#     if st.secrets is not None:
#         return Connection(
#             user=st.secrets["database"]["POSTGRES_USERNAME"],
#             password=st.secrets["database"]["POSTGRES_PASSWORD"],
#             database=st.secrets["database"]["POSTGRES_DATABASE"],
#             host=st.secrets["database"]["POSTGRES_HOSTNAME"],
#             port=int(st.secrets["database"]["POSTGRES_PORT"]),
#         )
#     else:
#         return Connection(
#                 user=os.getenv("POSTGRES_USERNAME"),
#                 password=os.getenv("POSTGRES_PASSWORD"),
#                 database=os.getenv("POSTGRES_DATABASE"),
#                 host=os.getenv("POSTGRES_HOSTNAME"),
#                 port=int(os.getenv("POSTGRES_PORT")),
#             )

# def get_connection():
#     return Connection(
#                 user=os.getenv("POSTGRES_USERNAME"),
#                 password=os.getenv("POSTGRES_PASSWORD"),
#                 database=os.getenv("POSTGRES_DATABASE"),
#                 host=os.getenv("POSTGRES_HOSTNAME"),
#                 port=int(os.getenv("POSTGRES_PORT")),
#             )


def run_query(query):
    conn = get_connection()
    result = conn.run(query)
    conn.close()
    return result


def get_column_names_for_tables(tables):
    # Create the SQL query to get column names for all tables
    query = f"""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_name IN ({','.join([f"'{table}'" for table in tables])});
    """
    
    connection = get_connection()
    result = connection.run(query)
    connection.close()

    # Convert the result into a DataFrame
    df = pd.DataFrame(result, columns=['table_name', 'column_name'])
    return df

def get_data_from_table(table_name, limit=10):
    #Fetch column names from the table
    query_columns = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
    """
    result_columns = run_query(query_columns)
    columns = [row[0] for row in result_columns]

    #Fetch actual data from the table
    query_data = f"SELECT * FROM {table_name} LIMIT {limit};"
    result_data = run_query(query_data)
    
    df = pd.DataFrame(result_data, columns=columns)

    if 'unit_price' in df.columns:
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    
    return df


# functions to display descriptive statistics for a table
def describe_table(df):
    st.subheader(f"Descriptive Statistics and Data Types for {selected_table_name}")

    # Display column data types
    st.write("**Column Data Types**:")
    st.write(df.dtypes)

    # Display descriptive statistics for numerical columns
    st.write("**Descriptive Statistics**:")
    st.write(df.describe())


# Function to check for null values in the DataFrame
def check_null_values(df, selected_table_name):
    st.subheader("Null Values Check")
    null_counts = df.isnull().sum()  # Count null values in each column
    st.write(null_counts)
    
    # Check if there are any null values
    if df.isnull().values.any():
        st.warning(f"There are null values in {selected_table_name}")
        null_handling_method = st.selectbox("How would you like to handle null values?", 
                                        ['Drop Rows', 'Drop Columns', 'Fill with 0', 'Fill with Mean', 'Do Nothing'])
        
        df_cleaned = handle_null_values(df, null_handling_method)

        #display the cleaned DataFrame
        st.subheader(f"Cleaned Data for {selected_table_name}")
        st.dataframe(df_cleaned)
    else:
        st.success(f"There are no null values in {selected_table_name}")


def handle_null_values(df, method):
    if method == 'Drop Rows':
        df_cleaned = df.dropna()
        st.write("Rows with null value has dropped")
    elif method == 'Drop Columns':
        df_cleaned == df.dropna(axis=1)
        st.write("Columns with null values have been dropped")
    elif method == 'Fill with 0':
        df_cleaned = df.fillna(0)
        st.write("Null values have been filled with 0.")
    elif method == 'Fill with Mean':
        for col in df.select_dtypes(include='number').columns:
            df[col] = df[col].fillna(df[col].mean())
        df_cleaned = df
        st.write("Null values have been filled with the column mean.")
    else:
        df_cleaned = df
        st.write("No null handling applied.")
    
    return df_cleaned



############ Seprate Sql analysis
# analyse sales performance by sales staff and location
# 'fact_sales_order', 'dim_staff', 'dim_location'
# Query: What is the total sales amount generated by each staff member at each country?
def get_total_sales_by_staff_at_location():
    query = """
        SELECT CONCAT(s.first_name, ' ', s.last_name) AS staff_name,
               l.country AS location_name,
               SUM(fso.units_sold * fso.unit_price) AS total_sales_amount
        FROM fact_sales_order fso
        JOIN dim_staff s ON fso.sales_staff_id = s.staff_id
        JOIN dim_location l ON fso.agreed_delivery_location_id = l.location_id
        GROUP BY s.first_name, s.last_name, l.country
        ORDER BY total_sales_amount DESC;
    """

    result = run_query(query)
    df = pd.DataFrame(result, columns=['staff_name', 'location_name', 'total_sales_amount'])
    # st.write(df.dtypes)

    df['total_sales_amount'] = pd.to_numeric(df['total_sales_amount'], errors='coerce')

    return df 

# Function to display statistics for the DataFrame
def display_statistics(df):
    st.subheader("Statistical Summary for Total Sales")
    
    # General summary statistics using describe()
    st.write("Summary Statistics:")
    st.write(df['total_sales_amount'].describe())

    # Display column data types
    st.write("**Column Data Types**:")
    st.write(df['total_sales_amount'].dtypes)


    
#############
primary_key_columns = {
    'fact_sales_order': 'sales_record_id',
    'dim_staff': 'staff_id',
    'dim_location': 'location_id',
    'dim_design': 'design_id',
    'dim_date': 'date_id',
    'dim_currency': 'currency_id',
    'dim_counterparty': 'counterparty_id'
}


st.title("Data Warehouse Dashboard")

st.subheader("Data Warehouse Column Viewer")

tables = list(primary_key_columns.keys())  #only table with known primary key are tables we are interested
df_columns = get_column_names_for_tables(tables)

selected_table_name = st.selectbox("Select table name to filter by", tables)

# filtered_table_columns = df_columns[df_columns['table_name'] == selected_table_name]['column_name']
filtered_table_columns = df_columns[df_columns['table_name'] == selected_table_name]

primary_key_column = primary_key_columns[selected_table_name]

df_filtered = filtered_table_columns.set_index(
    filtered_table_columns['column_name'] == primary_key_column)['column_name']


st.write(f"Columns in the selected table {selected_table_name}")
st.write(df_filtered)


#########
# Fetch and display the actual data from the selected table
st.subheader(f"Data from {selected_table_name}")

data = get_data_from_table(selected_table_name)
st.dataframe(data)  # interactive tables
# st.write(data)  # static table


###########
if st.button(f"Describe summary statistics for the table {selected_table_name}"):
    describe_table(data)


###########
if st.button(f"Check for Null Values in {selected_table_name}"):
    check_null_values(data, selected_table_name)


############# Analysis #############

# Define the SQL queries for analysis
# sql_queries = {
#     "Total sales by staff and location(Query: What is the total sales amount generated by each staff member at each country?)": """
#         SELECT CONCAT(s.first_name, ' ', s.last_name) AS staff_name,
#                l.country AS location_name,
#                SUM(fso.units_sold * fso.unit_price) AS total_sales_amount
#         FROM fact_sales_order fso
#         JOIN dim_staff s ON fso.sales_staff_id = s.staff_id
#         JOIN dim_location l ON fso.agreed_delivery_location_id = l.location_id
#         GROUP BY s.first_name, s.last_name, l.country
#         ORDER BY total_sales_amount DESC;
#     """,
#     "Sales by product design": """
#         SELECT d.design_name, 
#                SUM(fso.units_sold * fso.unit_price) AS total_sales_amount
#         FROM fact_sales_order fso
#         JOIN dim_design d ON fso.design_id = d.design_id
#         GROUP BY d.design_name
#         ORDER BY total_sales_amount DESC;
#     """,
#     "Sales by currency": """
#         SELECT c.currency_code, 
#                SUM(fso.units_sold * fso.unit_price) AS total_sales_amount
#         FROM fact_sales_order fso
#         JOIN dim_currency c ON fso.currency_id = c.currency_id
#         GROUP BY c.currency_code
#         ORDER BY total_sales_amount DESC;
#     """
# }

# # Add a dropdown for users to select a query
# st.subheader("Select a Query for Analysis")
# selected_query_name = st.selectbox("Choose a query", list(sql_queries.keys()))


# if selected_query_name:
#     # Get the corresponding SQL query
#     selected_query = sql_queries[selected_query_name]

#     # Convert result to DataFrame
#     if selected_query_name == "Total sales by staff and location":
#        result = run_query(selected_query)
#        df = pd.DataFrame(result, columns=['staff_name', 'location_name', 'total_sales_amount'])
#        # st.write(df.dtypes)

#        df['total_sales_amount'] = pd.to_numeric(df['total_sales_amount'], errors='coerce')

#     elif selected_query_name == "Sales by product design":
#         result = run_query(selected_query)
#         df = pd.DataFrame(result, columns=['design_name', 'total_sales_amount'])

#     elif selected_query_name == "Sales by currency":
#         result = run_query(selected_query)
#         df = pd.DataFrame(data, columns=['currency_code', 'total_sales_amount'])


#     # Display the DataFrame
#     st.dataframe(df)

#     # Perform specific analysis for each query
#     if selected_query_name == "Total sales by staff and location":
#         st.subheader("Total Sales by Staff and Location (Country)")

#         if df['total_sales_amount'].isnull().all():
#             st.error("No numeric data available to plot.")
#         else:
#             sales_by_staff_location = df.pivot(index='staff_name', columns='location_name', values='total_sales_amount').fillna(0)

#             fig, ax = plt.subplots(figsize=(10, 6))
#             sales_by_staff_location.plot(kind='bar', ax=ax)
#             ax.set_xlabel("Staff Member")
#             ax.set_ylabel("Total Sales Amount")
#             ax.set_title("Total Sales by Staff and Location")
#             plt.xticks(rotation=45, ha='right')
#             st.pyplot(fig)

#     elif selected_query_name == "Sales by product design":
#         st.subheader("Total Sales by Product Design")
#         fig, ax = plt.subplots(figsize=(10, 6))
#         df.plot(kind='bar', x='design_name', y='total_sales_amount', ax=ax)
#         ax.set_xlabel("Product Design")
#         ax.set_ylabel("Total Sales Amount")
#         ax.set_title("Sales by Product Design")
#         plt.xticks(rotation=45, ha='right')
#         st.pyplot(fig)
    
#     elif selected_query_name == "Sales by currency":
#         st.subheader("Total Sales by Currency")
#         fig, ax = plt.subplots(figsize=(10, 6))
#         df.plot(kind='bar', x='currency_code', y='total_sales_amount', ax=ax)
#         ax.set_xlabel("Currency")
#         ax.set_ylabel("Total Sales Amount")
#         ax.set_title("Sales by Currency")
#         plt.xticks(rotation=45, ha='right')
#         st.pyplot(fig)
    

    # # Display statistical summary for the result
    # st.subheader("Statistical Summary")
    # st.write(df.describe())



###########
st.subheader("Sales Analysis: Total Sales by Staff and Location")

if st.button("Run Sales Analysis"):
    sales_data = get_total_sales_by_staff_at_location()

    ##########
    st.subheader("Total Sales by Staff and Location(Contry)")
    st.dataframe(sales_data)

    if sales_data['total_sales_amount'].isnull().all():
        st.error("No numeric data available to plot.")
    else:
        #pivot the data into a new format; 
        # each row corresponds to a staff member, each column corresponds to a location
        # the cell values represent total sales amount for each staff memeber at each location
        # fill missing value with 0
        sales_by_staff_location = sales_data.pivot(index='staff_name', columns='location_name', values='total_sales_amount').fillna(0)

        #Create the Matplotlib Figure and Axes
        fig, ax = plt.subplots(figsize=(10, 6))

        #plot the grouped bar chart
        sales_by_staff_location.plot(kind='bar', ax=ax)  

        #Customize the Labels and Title
        ax.set_xlabel("Staff Member")
        ax.set_ylabel("Total Sales Amount")
        ax.set_title("Total Sales by Staff and Location")

        # rotate x-axis lables 45 degrees
        plt.xticks(rotation=45, ha='right')
        
        #render the matplot figure inside the streamlit app
        st.pyplot(fig)

        # Display statistical summary for the total_sales_amount column
        display_statistics(sales_data)
        





#############Embeded Tableau############

st.subheader("Streamlit Dashboard with Tableau Worksheets")

option = st.selectbox(
    'Would you like to load the Tableau dashboard?',
    ('No', 'Yes')
)

st.subheader("Refresh tableau worksheet")
if 'refresh' not in st.session_state:
    st.session_state.refresh = False

if st.button("Refresh Tableau Dashboard"):
    st.session_state.refresh = True

tableau_url = "https://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/authoring/Totesys_team_7_workbook/TotesysDashboard/Sheet%2015#2"


if option == 'Yes':
    st.components.v1.iframe(tableau_url, width=1200, height=800)
    st.write("Tableau dashboard loaded and refreshed.")
else:
    st.write("Select 'Yes' to load the Tableau dashboard.")