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
    query = f"SELECT * FROM {table_name} LIMIT {limit};"
    result = run_query(query)
    df = pd.DataFrame(result)
    
    return df



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

# Fetch and display the actual data from the selected table
st.subheader(f"Data from {selected_table_name}")

data = get_data_from_table(selected_table_name)
st.dataframe(data)

st.subheader("Streamlit Dashboard with Tableau Worksheets")


# def tableau_login_and_fetch():
#     # Access Tableau credentials from secrets
#     tableau_username = st.secrets["tableau"]["username"]
#     tableau_password = st.secrets["tableau"]["password"]
#     server_url = st.secrets["tableau"]["server_url"]
#     site_id = st.secrets["tableau"]["site_id"]

#     # Set up Tableau connection
#     config = {
#         'my_env': {
#             'server': server_url,
#             'api_version': '3.22', 
#             'username': tableau_username,
#             'password': tableau_password,
#             'site_name': site_id,
#             'site_url': site_id
#         }
#     }

#     conn = TableauServerConnection(config_json=config, env='my_env')
#     conn.sign_in()
    
#     if not conn.auth_token:
#         st.error("Authentication failed! No valid auth token was returned.")
#         return None
#     # Fetch views (worksheets/dashboards)
#     views_df = get_views_dataframe(conn)
    
#     # Close connection after fetching data
#     conn.sign_out()
    
#     return views_df

# # Display the result in Streamlit
# views = tableau_login_and_fetch()
# if views is not None:
#     st.write(views)

# if st.checkbox("Load Tableau Dashboard"):
#     tableau_url = "hhttps://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/authoring/Totesys_team_7_workbook/TotesysDashboard#4"
#     st.components.v1.iframe(tableau_url, width=1200, height=800)
#     st.write("Tableau dashboard loaded!")
# else:
#     st.write("Click the checkbox to load the Tableau dashboard.")

option = st.selectbox(
    'Would you like to load the Tableau dashboard?',
    ('No', 'Yes')
)

st.subheader("Refresh tableau worksheet")
if 'refresh' not in st.session_state:
    st.session_state.refresh = False

if st.button("Refresh Tableau Dashboard"):
    st.session_state.refresh = True

# tableau_url = "https://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/authoring/Totesys_team_7_workbook/TotesysDashboard#4"

tableau_url = "https://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/authoring/Totesys_team_7_workbook/TotesysDashboard/Sheet%2015#2"

# tableau_url = "https://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/views/Totesys_team_7_workbook/TotesysDashboard?:iid=3"

if option == 'Yes':
    st.components.v1.iframe(tableau_url, width=1200, height=800)
    st.write("Tableau dashboard loaded and refreshed.")
else:
    st.write("Select 'Yes' to load the Tableau dashboard.")