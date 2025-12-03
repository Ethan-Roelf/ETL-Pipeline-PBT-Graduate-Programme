import sqlalchemy
from sqlalchemy import INT, BOOLEAN, Table, Column, MetaData, ForeignKey,create_engine, text, VARCHAR, DATE, DECIMAL 

# Database connection variables
db_user = 'root'
db_password = 'Ethan200Gxi'
db_host = 'localhost'
db_port = 3306
db_name = 'VMobile_DB'


connection_string = (
    f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
)


# Create the engine object
engine = create_engine(connection_string)
metadata = MetaData() 


# 3 STAGING TABLES
tbl_vm = Table('vmobile_table', metadata,
    Column('uniqueid', INT, primary_key=True, autoincrement=True),
    Column('Location', VARCHAR(255)),
    Column('Cell_Number', VARCHAR(255)),
    Column('SIM_Activation_Date', VARCHAR(255)), 
    Column('First_Name', VARCHAR(255)),
    Column('Last_Name', VARCHAR(255)),
    Column('Birthday', VARCHAR(255)), 
    mysql_engine='InnoDB'
)

# BlueMobile Table
tbl_bm = Table('bluemobile_table', metadata,
    Column('uniqueid', INT, primary_key=True, autoincrement=True),
    Column('Activate', VARCHAR(255)), 
    Column('Name', VARCHAR(255)),
    Column('City', VARCHAR(255)),
    Column('Cell', VARCHAR(255)), 
    Column('Date', VARCHAR(255)), 
    Column('Surname', VARCHAR(255)),
    mysql_engine='InnoDB'
)

# ArrowMobile Table
tbl_am = Table('arrowmobile_table', metadata,
    Column('uniqueid', INT, primary_key=True, autoincrement=True),
    Column('CellNo', VARCHAR(255)),
    Column('FirstName', VARCHAR(255)),
    Column('LastName', VARCHAR(255)),
    Column('Area', VARCHAR(255)),
    Column('SIMDate', VARCHAR(255)), 
    mysql_engine='InnoDB'
)

# Combined subscriber table with duplicates
tbl_combined_master_record = Table('combined_table_master_record', metadata,
    Column('unique_id', INT, primary_key=True, autoincrement=True),
    Column('cell_phone_number', VARCHAR(10)),
    Column('first_name', VARCHAR(255)),
    Column('last_name', VARCHAR(255)),
    Column('date_of_birth', DATE),
    Column('sim_activation_date', DATE),
    Column('region', VARCHAR(50)),
    Column('source_system_name', VARCHAR(50)),
    Column('is_master_record', BOOLEAN),
    mysql_engine='InnoDB'
)

# Combined subscriber table without duplicates
tbl_combined_no_duplicates = Table('combined_table_no_duplicates', metadata,
    Column('cell_phone_number', VARCHAR(10), primary_key=True),
    Column('first_name', VARCHAR(255)),
    Column('last_name', VARCHAR(255)),
    Column('date_of_birth', DATE),
    Column('region', VARCHAR(50)),
    Column('sim_activation_date', DATE),
    Column('source_system_name', VARCHAR(50)),
    mysql_engine='InnoDB'
)

# Table containing qualified subscribers
tbl_qualifying_subscriber = Table('qualifying_subscriber_table', metadata,
    Column('fact_id', INT, primary_key=True, autoincrement=True),
    Column('cell_phone_number_fk', VARCHAR(10), ForeignKey('combined_table_no_duplicates.cell_phone_number')), 
    Column('reporting_date', DATE),
    Column('total_revenue', DECIMAL(10, 2)),
    Column('total_call_count', INT),
    Column('total_sms_count', INT),
    Column('region', VARCHAR(50)),
    mysql_engine='InnoDB'
)


# Create all tables
metadata.create_all(engine)