**VMobile Subscriber & Campaign Analysis**

An end-to-end Data Engineering pipeline designed to consolidate fragmented subscriber data and analyze "Free Minutes" campaign performance for VMobile. This project implements a professional Medallion Architecture to transform raw transactional data into actionable business intelligence.

Project Overview (Full business Use-case and requirements can be found under in "BRD" document)

The goal of this pipeline is to ingest data from multiple source systems (VMobile, BlueMobile, ArrowMobile), resolve customer duplicates through Master Data Management (MDM), and identify "Qualifying Subscribers" based on weekly revenue thresholds (R30+).

**Architecture: Medallion Strategy**

The project follows a structured data flow to ensure quality and auditability:

Bronze (Raw): Landing zone for original CSV files. Data is stored in 100% fidelity to the source in staging tables.

Silver (Standardized): Data is cleaned and standardized. This includes, but is not limited to - eliminating vague column names, standardizing all cellphone number formats, removing duplicate records and handling inconsistent delimiters( semicolons vs. commas) 

Gold (Curated): A high-performance Star Schema is generated. This layer consists of the Combined_table_no_duplicates (Dimension table) and Qualifying_Subscriber_table (Fact table), optimized for rapid reporting.

**Tech Stack**

Language: Python

Processing: Pandas (ETL Logic)

Database: MySQL (Storage & Warehousing)

Interface: SQLAlchemy & PyMySQL

Reporting: XlsxWriter (Automated Excel generation)

**Key Features**

Automated MDM Logic: Prioritizes records based on source system hierarchy and SIM activation recency.

Robust Data Cleaning: Handles inconsistent delimiters (semicolons vs. commas) and varying column headers across platforms.

Professional Reporting: Generates a formatted Excel Executive Summary for the Marketing Department.
