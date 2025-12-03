import pandas as pd
from sqlalchemy import create_engine, text, VARCHAR, DATE, DECIMAL, INT, BOOLEAN
from sqlalchemy.exc import SQLAlchemyError
import numpy as np
import os

# Configuration
db_user = 'root'
db_password = 'Ethan200Gxi' 
db_host = 'localhost'
db_port = 3306
db_name = 'VMobile_DB'
connection_string = (
    f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# File paths and table names
# Dictionary is used so that when the loading staging tables function is called, it can just iterate through this dictionary
file_map = {
    'vmobile_table': {
        'file': '/Users/ethanroelf/Desktop/VMobile_Solution/Data/raw/VMobile_subscribers.csv', 
        'source_name': 'VMobile',
    },
    'bluemobile_table': {
        'file': '/Users/ethanroelf/Desktop/VMobile_Solution/Data/raw/VMobile_subscribers_bluemobile.csv',
        'source_name': 'BlueMobile',
    },
    'arrowmobile_table': {
        'file': '/Users/ethanroelf/Desktop/VMobile_Solution/Data/raw/VMobile_subscribers_arrowmobile.csv',
        'source_name': 'ArrowMobile',
    }
}

# Paths required to load qualifying subscribers table
usage_records_paths = [
    '/Users/ethanroelf/Desktop/VMobile_Solution/Data/raw/VMobile_usage_records.csv',
    '/Users/ethanroelf/Desktop/VMobile_Solution/Data/raw/VMobile_usage_records_week_2.csv'
]
usage_lookup_path = '/Users/ethanroelf/Desktop/VMobile_Solution/Data/raw/VMobile_usage_event_lookup.csv'
qualifying_table = 'qualifying_subscriber_table'

report_output_path = '/Users/ethanroelf/Desktop/VMobile_Solution/Reports/Weekly_Subscriber_Report.xlsx'


# System Priority for master record
source_priority = {
    'VMobile': 1,
    'BlueMobile': 0,
    'ArrowMobile': 0
}

# Qualification criteria ofr subscriber
min_total_revenue = 30.00  



# Global Cleaning functions for messy column formats and data


#Renames columns from CSV files to SQL standard
def clean_headers(df):
    new_columns = {}
    for col in df.columns:
        new_col = col.strip()
        new_col = ''.join(e if e.isalnum() or e == '_' else '_' for e in col)
        new_col = new_col.replace('__', '_').strip('_')
        new_columns[col] = new_col
    df.rename(columns=new_columns, inplace=True)
    return df

#Formats cellphone number and date columns
def clean_and_process_data(df):
    
    
    df['cell_phone_number'] = df['cell_phone_number'].astype(str).str.replace(r'[^\d]', '', regex=True)
    
    # handles numbers that start with "27"
    df.loc[df['cell_phone_number'].str.len() == 11, 'cell_phone_number'] = \
        df.loc[df['cell_phone_number'].str.len() == 11, 'cell_phone_number'].str[2:]
        
    # handles numbers that dont have "27" or start with "0" 
    df.loc[df['cell_phone_number'].str.len() == 9, 'cell_phone_number'] = \
        '0' + df.loc[df['cell_phone_number'].str.len() == 9, 'cell_phone_number']
        
    #date and priority
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%d %m %Y', errors='coerce')
    df['sim_activation_date'] = pd.to_datetime(df['sim_activation_date'], format='%d %m %Y', errors='coerce')
    df['source_priority'] = df['source_system_name'].map(source_priority)
    
    return df

# Standardizes the cellphone number found in the usage reports - Did not want to try and reuse the earlier standardized column/clean and process function, as I am now dealing with different column names. This also makes the code
def standardize_msisdn(df, column_name='cell_phone_number'):
    
    df[column_name] = df[column_name].astype(str).str.replace(r'[^\d]', '', regex=True)
    
    # Check for the 11 digit format "27"
    df.loc[df[column_name].str.len() == 11, column_name] = \
        df.loc[df[column_name].str.len() == 11, column_name].str[2:]
        
    # Check for 9 digit format
    df.loc[df[column_name].str.len() == 9, column_name] = \
        '0' + df.loc[df[column_name].str.len() == 9, column_name]
        
    return df


# LOADING RAW DATA INTO STAGING TABLES

def load_staging_tables(engine):
    
    for table_name, config in file_map.items():
        file_path = config['file']
        source_system = config['source_name'] 
        print(f"\nProcessing {source_system} data from {file_path}")

#create dataframe for staging tables, clean column headers, and create a clean copy of the df
        try:
            df = pd.read_csv(file_path, keep_default_na=False, sep=';', header=0) 
            df = clean_headers(df)
            df_to_load = df.copy() 
            
            with engine.connect() as connection:
                connection.execute(text(f"TRUNCATE TABLE `{table_name}`"))
                connection.commit() 
                print(f"   -> Table {table_name} truncated.")
            
#This map and load the dataframe to the SQL database tables
            with engine.begin() as connection:
                df_to_load.to_sql(table_name, connection, if_exists='append', index=False) # This maps the dataframe to the SQL database tables
                
            print(f"Successfully loaded {table_name}")
            
        except Exception as e:
            print(f"Error processing {table_name}: {e}")
            raise 

    print(" Staging tables loaded.")



#  THE RAW SUBSCRIBER DATA IS NOW SUCCESSFULLY EXTACTED INTO THE MYSQL STAGING TABLES
# -------------------------------------------------------------------------------------------------


# This next section involves combining the data from the 3 staging tables in order to conduct the master data management logic

def apply_mdm_logic(engine):
    

    all_dfs = []
    
    # Extract staging table data and standardize columns in order to be populated into clean duplicates table
    for table_name, config in file_map.items():
        try:
            # Pull data from each staging table 
            df = pd.read_sql_table(table_name, engine)
            
            # Rename columns to meet MySQL standards - this is in order to map it to a consistent column name in the combined table with duplicates           
            column_rename_map = {
                'Location': 'region', 'Cell_Number': 'cell_phone_number', 'SIM_Activation_Date': 'sim_activation_date', 'First_Name': 'first_name', 'Last_Name': 'last_name', 'Birthday': 'date_of_birth', # VMobile
                'City': 'region', 'Cell': 'cell_phone_number', 'Activate': 'sim_activation_date', 'Name': 'first_name', 'Surname': 'last_name', 'Date': 'date_of_birth', # BlueMobile
                'Area': 'region', 'CellNo': 'cell_phone_number', 'SIMDate': 'sim_activation_date', 'FirstName': 'first_name', 'LastName': 'last_name', # ArrowMobile
            }
            filtered_map = {k: v for k, v in column_rename_map.items() if k in df.columns}
            df.rename(columns=filtered_map, inplace=True)

            df['source_system_name'] = config['source_name']

#Append every dataframe to the combined dataframes list            
            all_dfs.append(df)
            print(f"   -> Extracted and tagged {len(df)} records from {table_name}")

        except Exception as e:
            print(f"Error with {table_name}: {e}")
            raise 

    df_combined = pd.concat(all_dfs, ignore_index=True, sort=False)
    
    # Resets the row index to ensure that its incrementing normally, with no duplicates from the original 3 separate dataframes
    df_combined.reset_index(drop=True, inplace=True)

    # Cleans the date and cellphone number formats and also adds the source column
    df_combined = clean_and_process_data(df_combined)

    
    # LOGIC TO IDENTIFY MASTER RECORDS
    #-------------------------------------------------
    
    # This sorts all the records according to the master record criteria
    df_sorted = df_combined.sort_values(
        by=['source_priority', 'sim_activation_date'],
        ascending=[False, False]
    ).copy()
    
    #Select the first record in the sorted group for each unique cellphone number
    # Create a dataframe which will store only the master records
    master_df = df_sorted.drop_duplicates(
        subset=['cell_phone_number'],
        keep='first'
    )
    
    # Get the index values of all the master records
    master_index_values = master_df.index.values

    # Set the master record column = False for all records
    df_combined['is_master_record'] = False
    
    # Go back to the original combined dataframe and set the identified master records to TRUE
    df_combined.loc[master_index_values, 'is_master_record'] = True
    
    # Drop the temporary priority column as its not needed after the sorting is done
    df_combined.drop(columns=['source_priority'], inplace=True)
    

    print(f"All records processed and Master Records Identified")


    # ----------------------------------------------------
    # LOAD AND POPULATE COMBINED DUPLICATES TABLE
    # ----------------------------------------------------


    #Finalize columns for the combined table schema
    df_combined_final = df_combined[[
        'cell_phone_number', 'first_name', 'last_name', 'date_of_birth', 
        'sim_activation_date', 'region', 'source_system_name', 'is_master_record'
    ]].copy()
    
    # Ensure all date columns are properly formatted before loading
    df_combined_final['date_of_birth'] = df_combined_final['date_of_birth'].dt.date
    df_combined_final['sim_activation_date'] = df_combined_final['sim_activation_date'].dt.date
    
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE `combined_table_master_record`"))
        connection.commit() 
        
    # Bulk Load into the combined_table_master_record duplicates
    with engine.begin() as connection:
        df_combined_final.to_sql(
            'combined_table_master_record', 
            connection, 
            if_exists='append', 
            index=False, 
            dtype={'is_master_record': BOOLEAN}
        )
        
    print(f" Loaded {len(df_combined_final)} records into combined_table_master_record")



# Loading the final combined table with no duplicates
# ----------------------------------------------------


def load_no_duplicates_table(engine):
   
    final_combined_table = 'combined_table_no_duplicates' 
    qualifying_table = 'qualifying_subscriber_table' 

 #Used AI to propose a solution here, because I was getting errors with keeping the primary and foreign key in tact. This is because of the ".tosql" method
    
    try:
        # Truncate the Qualifying Table first before dropping the combined table (due to FK constraint)
        with engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE `{qualifying_table}`"))
            connection.commit() 
        
        #Temporarily disable foreign key checks to successfully drop table
        with engine.connect() as connection:
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            connection.commit() 

        # Extract Master Records
        query_master_records = text("""
            SELECT 
                cell_phone_number, 
                first_name, 
                last_name, 
                date_of_birth, 
                sim_activation_date, 
                region, 
                source_system_name
            FROM 
                combined_table_master_record
            WHERE 
                is_master_record = TRUE
        """)

        with engine.connect() as connection:
            df_master = pd.read_sql(query_master_records, connection)
            
        # For some reason I couldnt TRUNCATE the combined table with no duplicates and got the following error: 'Cannot truncate a table referenced in a foreign key constraint (`VMobile_DB`.`Qualifying_Subscriber_table`, CONSTRAINT `qualifying_subscriber_table_ibfk_1`)')
        # This forced me to DROP and RECREATE the table IN ORDER TO MAINTAIN THE FOREIGN KEY CONSTRAINT which was essential to keep to answer the marketing departments business question
        # Learned about idempotency

        create_table_sql = f"""
            CREATE TABLE `{final_combined_table}` (
                `cell_phone_number` VARCHAR(10) NOT NULL,
                `first_name` VARCHAR(100),
                `last_name` VARCHAR(100),
                `date_of_birth` DATE,
                `sim_activation_date` DATE,
                `region` VARCHAR(50),
                `source_system_name` VARCHAR(50),
                PRIMARY KEY (`cell_phone_number`)
            )
        """
        
        with engine.connect() as connection:
            # Drop the table
            connection.execute(text(f"DROP TABLE IF EXISTS `{final_combined_table}`"))
            
            # Create the table with the PRIMARY KEY
            connection.execute(text(create_table_sql))
            connection.commit() 

        # Load Data
        df_no_duplicates = df_master.copy()
        
        # Define dtypes for the APPEND operation to avoid inserting text again
        dtype_mapping = {
            'cell_phone_number': VARCHAR(10), 
            'first_name': VARCHAR(100),
            'last_name': VARCHAR(100),
            'region': VARCHAR(50),
            'source_system_name': VARCHAR(50)
        }

        #Inserting new data
        with engine.begin() as connection:
            df_no_duplicates.to_sql(
                final_combined_table, 
                connection, 
                if_exists='append', # Use append since we manually dropped and created
                index=False,
                dtype=dtype_mapping
            )
            
        # Re-enable foreign key checks immediately after the load
        with engine.connect() as connection:
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            connection.commit() 

        print(f"Loaded {final_combined_table}")

    except Exception as e:
        print(f"Error details: {e}")
        raise
        



# LOAD THE QUALIFYING SUBSCRIBERS TABLE
# ----------------------------------------------------



def load_qualifying_subscriber_table(engine, usage_records_paths, lookup_file_path):
    """
    Loads usage data, performs aggregation, identifies qualifying subscribers
    ONLY for the weeks where they generated R30 or more.
    """
    no_duplicates_table = 'combined_table_no_duplicates'
    qualifying_table = 'qualifying_subscriber_table'
    
    # Helper function to find and rename columns
    def find_and_rename_col(df, keywords, target_name):
        keywords = [k.upper() for k in keywords]
        found_col = None
        for col in df.columns:
            if any(keyword in col.upper() for keyword in keywords):
                found_col = col
                break
        if not found_col:
            raise ValueError(f"Could not find required column {keywords}")
        df.rename(columns={found_col: target_name}, inplace=True)
        return df

    def read_file_safe(file_path):
        
        df = pd.read_csv(file_path, keep_default_na=False, sep=';', header=0, dtype=str)
        if len(df.columns) > 1: 
            return df
        
        df_comma = pd.read_csv(file_path, keep_default_na=False, sep=',', header=0, dtype=str)
        if len(df_comma.columns) > 1: 
            return df_comma
        
        return df
        

    try:
        # Load Lookup
        df_lookup = pd.read_csv(lookup_file_path, sep=';') 
        df_lookup.columns = ['usage_event_type_id', 'usage_type'] 
        
        all_usage_dfs = []

        for file_path in usage_records_paths:

            df = read_file_safe(file_path)
            df = clean_headers(df)
            
            # Rename critical columns
            try:
                df = find_and_rename_col(df, ['MSISDN'], 'cell_phone_number')
                df = find_and_rename_col(df, ['USAGE_EVENT_TYPE_ID'], 'usage_event_type_id')
                df = find_and_rename_col(df, ['USAGE_EVENT_REVENUE'], 'usage_event_revenue')
                df = find_and_rename_col(df, ['USAGE_EVENT_DATE_TIME'], 'usage_event_date_time')
            except ValueError as e:
                raise Exception(f"Failed to identify critical columns in {file_path}: {e}")
            

            df = standardize_msisdn(df, column_name='cell_phone_number')
            
            # Ensure numeric columns are numeric for calculation
            df['usage_event_type_id'] = pd.to_numeric(df['usage_event_type_id'], errors='coerce')
            df['usage_event_revenue'] = pd.to_numeric(df['usage_event_revenue'], errors='coerce').fillna(0)

            # Join with lookup to differentiate between sms and voice calls
            df = df.merge(df_lookup, on='usage_event_type_id', how='left')
            
            # Parse Dates
            df['parsed_date'] = pd.to_datetime(df['usage_event_date_time'], dayfirst=True, errors='coerce')
            

            # Calculate week start and end dates which is required for the final report
            df['week_start_date'] = df['parsed_date'] - pd.to_timedelta(df['parsed_date'].dt.dayofweek, unit='D')
            df['week_end_date'] = df['week_start_date'] + pd.to_timedelta(6, unit='D')
            
            df['week_end_date'] = df['week_end_date'].dt.date
            df['week_start_date'] = df['week_start_date'].dt.date
            
            all_usage_dfs.append(df)

        # Combine all usage reports
        df_usage_combined = pd.concat(all_usage_dfs, ignore_index=True)
        print(f"   -> Combined total of {len(df_usage_combined)} usage records.")

        # Aggregation Logic
        df_usage_combined['is_sms'] = df_usage_combined['usage_type'].str.contains('sms', case=False, na=False)
        df_usage_combined['is_call'] = df_usage_combined['usage_type'].str.contains('call', case=False, na=False)
        
        #Calculate the weekly aggregates per user per week
        df_aggregate = df_usage_combined.groupby(['cell_phone_number','week_end_date']).agg(
            total_revenue=('usage_event_revenue', 'sum'),
            total_sms_count=('is_sms', 'sum'),
            total_call_count=('is_call', 'sum')
        ).reset_index()

        # Find qualifying subscribers who generated revenue of R30 or more
        df_qualified_usage = df_aggregate[df_aggregate['total_revenue'] >= min_total_revenue].copy()
        

        # Pull combined table attributes
        query_combined_attributes = text(f"""
            SELECT 
                cell_phone_number AS cell_phone_number_fk, 
                region
            FROM 
                `{no_duplicates_table}`
        """)
        
        with engine.connect() as connection:
            df_combined_attr = pd.read_sql(query_combined_attributes, connection)
            df_combined_attr['cell_phone_number_fk'] = df_combined_attr['cell_phone_number_fk'].astype(str)

        # Join to Master Table
        df_qualifying = df_qualified_usage.merge(
            df_combined_attr, 
            left_on='cell_phone_number', 
            right_on='cell_phone_number_fk',
            how='inner'
        )
        
        df_qualifying.rename(columns={'week_end_date': 'reporting_date'}, inplace=True)

        # Final qualifying table prep
        df_qualifying_final = df_qualifying[[
            'cell_phone_number_fk',
            'region',
            'reporting_date',
            'total_revenue', 
            'total_sms_count',
            'total_call_count'
        ]].copy()
        
        with engine.connect() as connection:
            connection.execute(text(f"TRUNCATE TABLE `{qualifying_table}`"))
            connection.commit()
            
        with engine.begin() as connection:
            qualifying_dtype_mapping = {
                'cell_phone_number_fk': VARCHAR(10), 
                'region': VARCHAR(50),
                'reporting_date': DATE,
                'total_revenue': DECIMAL(10, 2),
                'total_sms_count': INT,
                'total_call_count': INT
            }
            
            df_qualifying_final.to_sql(
                qualifying_table, 
                connection, 
                if_exists='append', 
                index=False,
                dtype=qualifying_dtype_mapping
            )
            
        print(f"Loaded {qualifying_table}")

    except Exception as e:
        print(f"Error details in load_qualifying_subscriber_table: {e}")
        raise



# REPORT GENERATION
# ----------------------------------------------------

def generate_excel_report(engine, output_path):
    
    # Updated Query to include T2.Region for location analysis
    query = text("""
        SELECT
            T1.Reporting_Date,
            T2.Cell_Phone_Number,
            T2.First_Name,
            T2.Last_Name,
            T2.Region,
            T1.Total_Revenue,
            T1.Total_SMS_Count,
            T1.Total_Call_Count
        FROM Qualifying_Subscriber_table AS T1
        INNER JOIN Combined_table_no_duplicates AS T2
        ON T1.Cell_Phone_Number_FK = T2.Cell_Phone_Number
        ORDER BY T1.Reporting_Date DESC, T1.Total_Revenue DESC
    """)
    
    try:
        with engine.connect() as connection:
            df_report = pd.read_sql(query, connection)

        if df_report.empty:
            print("No data found for report generation.")
            return

        # Calculate week start and end dates again
        df_report['Reporting_Date'] = pd.to_datetime(df_report['Reporting_Date'])
        
        df_report['Week End Date'] = df_report['Reporting_Date'].dt.strftime('%Y%m%d')
       
        df_report['Week Start Date'] = (df_report['Reporting_Date'] - pd.Timedelta(days=6)).dt.strftime('%Y%m%d')
        df_report['Reporting Date'] = df_report['Reporting_Date'].dt.strftime('%Y%m%d')

        # Final column names for the report (Including Region and Dates as columns)
        final_cols = ['Reporting Date', 'Week Start Date', 'Week End Date', 
                      'Region', 'Cell_Phone_Number', 'First_Name', 'Last_Name', 
                      'Total_Revenue', 'Total_SMS_Count', 'Total_Call_Count']
        
        # handling case sensitivity if sql returns lowercase
        if 'Region' not in df_report.columns and 'region' in df_report.columns:
             df_report.rename(columns={'region': 'Region'}, inplace=True)

        df_final = df_report[final_cols]

        # Generate excel report
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Formats
            header_fmt = workbook.add_format({'bold': True, 'fg_color': '#D7E4BC', 'border': 1})
        
            sheet_name_detail = 'Weekly Report'
            df_final.to_excel(writer, sheet_name=sheet_name_detail, index=False, startrow=0)
            
            worksheet_detail = writer.sheets[sheet_name_detail]
            for col_num, value in enumerate(df_final.columns.values):
                worksheet_detail.write(0, col_num, value, header_fmt)
                worksheet_detail.set_column(col_num, col_num, 18)

        print(f"Report Generated")

    except Exception as e:
        print(f"Error generating report: {e}")
        raise


# --- FINAL EXECUTION ---
# ----------------------------------------------------

if __name__ == '__main__':
    try:
        engine = create_engine(connection_string)

        #Extract raw data from CSV files into staging tables
        load_staging_tables(engine)
        
        print("\n" + "="*50)
        print("RAW DATA EXTRACTED AND STAGING TABLES LOADED")

        #Apply master record logic and load duplicates table
        apply_mdm_logic(engine)

        print("\n" + "="*50)
        print("MASTER RECORDS IDENTIFIED AND COMBINED TABLE LOADED")
        
        # Load combined table with no duplicates
        load_no_duplicates_table(engine)
        
        print("\n" + "="*50)
        print("COMBINED TABLE WITH NO DUPLICATES LOADED")
        
        #Load qualifying subscribers table
        load_qualifying_subscriber_table(engine, usage_records_paths, usage_lookup_path) 

        print("\n" + "="*50)
        print("QUALIFYING SUBSCRIBERS TABLE LOADED")
        
        #Report generation
        generate_excel_report(engine, report_output_path)
        
        print("\n REPORT GENERATED")
        
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Details: {e}")
    except Exception as e:
        print(f"Error Details: {e}")
