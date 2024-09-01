from io import BytesIO
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import pyarrow as pa 
import boto3

from s3_loader import load_data_from_s3
from dotenv import load_dotenv
import os

if os.path.exists('.env'):
    load_dotenv()

def get_env_var(var_name, default=None):
    return os.getenv(var_name) or st.secrets.get(var_name, default)

# BUCKET_NAME = os.environ.get("DATA_BUCKET_NAME")

AWS_ACCESS_KEY_ID = get_env_var('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = get_env_var('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = get_env_var('AWS_DEFAULT_REGION')
BUCKET_NAME = get_env_var('DATA_BUCKET_NAME')

s3_folder = "db/parquet_files"

data = load_data_from_s3(BUCKET_NAME, s3_folder)


# Initialize session state for data storage
if 'data' not in st.session_state:
    st.session_state.data = load_data_from_s3(BUCKET_NAME, s3_folder)
    st.success("Data loaded from AWS S3")

# Button to reload the latest data from S3
if st.button("Reload Data from AWS S3"):
    st.session_state.data = load_data_from_s3(BUCKET_NAME, s3_folder)
    st.success("Data reloaded from S3")


# Use data from session state
data = st.session_state.data


# # Functions to interact with pre-loaded data
def get_data_from_table(table_name, limit = 10):
    return data[table_name].head(limit)

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

def sales_by_staff_and_location(data):
    df_fso = data['fact_sales_order']
    df_staff = data['dim_staff']
    df_location = data['dim_location']

    df = pd.merge(df_fso, df_staff, left_on='sales_staff_id', right_on='staff_id')
    df = pd.merge(df, df_location, left_on='agreed_delivery_location_id', right_on='location_id')

    # Renaming 'country' column to 'location_name' 
    df.rename(columns={'country': 'location_name'}, inplace=True)

    df['total_sales_amount'] = df['units_sold'] * df['unit_price']

    df = df.groupby(['first_name', 'last_name', 'location_name']).agg(total_sales_amount=('total_sales_amount', 'sum')).reset_index()

    df['staff_name'] = df['first_name'] + ' ' + df['last_name']

    df['total_sales_amount'] = pd.to_numeric(df['total_sales_amount'], errors='coerce')

    # Sort by 'total_sales_amount' in descending order, without the old index being added as a column
    df = df.sort_values(by='total_sales_amount', ascending=False).reset_index(drop=True)

    return df[['staff_name', 'location_name', 'total_sales_amount']]


def sales_by_product_design(data):
    df_fso = data['fact_sales_order']
    df_design = data['dim_design']

    df = pd.merge(df_fso, df_design, on='design_id')

    df['total_sales_amount'] = df['units_sold'] * df['unit_price']

    df = df.groupby('design_name').agg(total_sales_amount=('total_sales_amount', 'sum')).reset_index()

    df['total_sales_amount'] = pd.to_numeric(df['total_sales_amount'], errors='coerce')

    # Sort by 'total_sales_amount' in descending order, without the old index being added as a column
    df = df.sort_values(by='total_sales_amount', ascending=False).reset_index(drop=True)

    return df


def sales_by_currency(data):
    df_fso = data['fact_sales_order']
    df_currency = data['dim_currency']

    df = pd.merge(df_fso, df_currency, on='currency_id')

    df['total_sales_amount'] = df['units_sold'] * df['unit_price']

    df = df.groupby('currency_code').agg(total_sales_amount=('total_sales_amount', 'sum')).reset_index()

    # Sort by 'total_sales_amount' in descending order, without the old index being added as a column
    df = df.sort_values(by='total_sales_amount', ascending=False).reset_index(drop=True)

    return df


# Function to display statistics for the DataFrame
def describe_table(df):
    st.subheader("Descriptive Statistics and Data Types")
    st.write("**Column data types**")
    st.write(df.dtypes)  # this works in the app production, but there is error message "Serialization of dataframe to Arrow table was unsuccessful" 
    st.write("**Descriptive Statistics**")
    st.write(df.describe(include=['number']))



def plot_bar_chart(df, x_column, y_column, title, x_label, y_label):
    """
    Plots a bar chart from a DataFrame and displays it using Streamlit.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the data to plot.
    x_column (str): The column name for the x-axis.
    y_column (str): The column name for the y-axis.
    title (str): The title of the chart.
    x_label (str): The label for the x-axis.
    y_label (str): The label for the y-axis.
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    df.plot(kind='bar', x=x_column, y=y_column, ax=ax)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.set_title(title)
    plt.xticks(rotation=45, ha='right')
    st.pyplot(fig)


# Function to display Statistical Summary for Total Sales
def display_total_sales_statistics(df):
    # General summary statistics using describe()
    st.write("Summary Statistics:")
    st.write(df['total_sales_amount'].describe())

    # Display column data types
    st.write("**Column Data Types**:")
    st.write(df['total_sales_amount'].dtypes)

###############
# Streamlit Interface
st.title("Data Warehouse Dashboard")

st.subheader("Data Warehouse Column Viewer")


############
primary_key_columns = {
    'fact_sales_order': 'sales_record_id',
    'dim_staff': 'staff_id',
    'dim_location': 'location_id',
    'dim_design': 'design_id',
    'dim_date': 'date_id',
    'dim_currency': 'currency_id',
    'dim_counterparty': 'counterparty_id'
}

tables = list(primary_key_columns.keys())

selected_table_name = st.selectbox("Select table name to filter by", tables)

data_table = data[selected_table_name]



st.subheader(f"Columns in {selected_table_name}")

# Extract column names
columns = data_table.columns

# Highlight the primary key
primary_key = primary_key_columns[selected_table_name]

# Display each column with primary key highlighted
for column in columns:
    if column == primary_key:
        st.markdown(f"**{column}** (Primary Key)")
    else:
        st.markdown(column)


############
st.subheader(f"Data from {selected_table_name}")

st.dataframe(data_table)


################
if st.button(f"Describe summary statistics for the table {selected_table_name}"):
    describe_table(data_table)

 
###########
if st.button(f"Check for Null Values in {selected_table_name}"):
    check_null_values(data_table, selected_table_name)




sql_queries = {
    "Sales by staff and location": sales_by_staff_and_location,
    "Sales by product design": sales_by_product_design,
    "Sales by currency": sales_by_currency
}

# Add a dropdown for users to select a query
st.subheader("Select a Query for Sale Analysis")
selected_query_name = st.selectbox("Choose a query", list(sql_queries.keys()))

if selected_query_name:
    # Get the corresponding SQL query
    selected_query = sql_queries[selected_query_name]
    df = selected_query(data)

    df['total_sales_amount'] = pd.to_numeric(df['total_sales_amount'], errors='coerce')

     # Perform specific analysis for each query
    if selected_query_name == "Sales by staff and location":
        st.subheader("Total Sales by Staff and Location (Country)")
        st.dataframe(df)
        

        if df['total_sales_amount'].isnull().all():
            st.error("No numeric data available to plot.")
        else:
            sales_by_staff_location = df.pivot(index='staff_name', columns='location_name', values='total_sales_amount').fillna(0)

            plot_bar_chart(
                df=sales_by_staff_location.reset_index(),
                x_column='staff_name',
                y_column=sales_by_staff_location.columns,  # All columns are countries
                title="Total Sales by Staff and Location",
                x_label="Staff Member",
                y_label="Total Sales Amount"
            )

            # Display statistical summary for the result
            st.subheader(f"Statistical Summary for Total Sales by Staff and Location (Country)")
            # st.write(df.describe())
            display_total_sales_statistics(df)
       

    elif selected_query_name == "Sales by product design":
        st.subheader("Total Sales by Product Design")
        st.dataframe(df)


        if df['total_sales_amount'].isnull().all():
            st.error("No numeric data available to plot.")
        else:
            plot_bar_chart(
                df=df,
                x_column='design_name',
                y_column='total_sales_amount',
                title="Sales by Product Design",
                x_label="Product Design",
                y_label="Total Sales Amount"
            )

            st.subheader(f"Statistical Summary for Total Sales by Product Design")
            display_total_sales_statistics(df)
    
    elif selected_query_name == "Sales by currency":
        st.subheader("Total Sales by Currency")
        st.dataframe(df)


        if df['total_sales_amount'].isnull().all():
            st.error("No numeric data available to plot.")
        else:
            plot_bar_chart(
                df=df,
                x_column='currency_code',
                y_column='total_sales_amount',
                title="Sales by Currency",
                x_label="Currency",
                y_label="Total Sales Amount"
            )

            st.subheader(f"Statistical Summary for Total Sales by Currency")
            display_total_sales_statistics(df)



############
st.subheader("Streamlit Dashboard Website Link")

tableau_url_another = "https://prod-uk-a.online.tableau.com/#/site/beveridgerraa063aab21/views/Totesys_team_7_workbook/TotesysDashboard2?:iid=1"

st.markdown(f'[Click here to open the Tableau Dashboard in a new tab]({tableau_url_another})')



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

# tableau_url = "https://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/authoring/Totesys_team_7_workbook/TotesysDashboard/Sheet%2015#2"

tableau_url = "https://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/authoring/Totesys_team_7_workbook/TotesysDashboard2#2"

# tableau_url = "https://prod-uk-a.online.tableau.com/#/site/beveridgerraa063aab21/workbooks/1243788/views"

# tableau_url_another = "https://prod-uk-a.online.tableau.com/t/beveridgerraa063aab21/authoring/Totesys_team_7_workbook/TotesysDashboard2#2"

# # tableau_url_another = "https://prod-uk-a.online.tableau.com/#/site/beveridgerraa063aab21/workbooks/1243788/views"


if option == 'Yes':
    st.components.v1.iframe(tableau_url, width=1200, height=800)
    st.write("Tableau dashboard loaded and refreshed.")
else:
    st.write("Select 'Yes' to load the Tableau dashboard.")

