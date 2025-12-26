VMobile Subscriber & Campaign Analysis

An end-to-end Data Engineering pipeline designed to consolidate fragmented subscriber data and analyze "Free Minutes" campaign performance for VMobile. This project implements a professional Medallion Architecture to transform raw transactional data into actionable business intelligence.

🚀 Project Overview

The goal of this pipeline is to ingest data from multiple source systems (VMobile, BlueMobile, ArrowMobile), resolve customer duplicates through Master Data Management (MDM), and identify "Qualifying Subscribers" based on weekly revenue thresholds (R30+).

🏗️ Architecture: Medallion Strategy

The project follows a structured data flow to ensure quality and auditability:

Bronze (Raw): Landing zone for original CSV files. Data is stored in 100% fidelity to the source in staging tables.

Silver (Standardized): Data is cleaned, PII is handled for POPIA compliance, and phone numbers are standardized to a 10-digit local format.

Gold (Curated): A high-performance Star Schema is generated. This layer consists of the Combined_table_no_duplicates (Dimension) and Qualifying_Subscriber_table (Fact), optimized for rapid reporting.

🛠️ Tech Stack

Language: Python 3.x

Processing: Pandas (ETL Logic)

Database: MySQL (Storage & Warehousing)

Interface: SQLAlchemy & PyMySQL

Reporting: XlsxWriter (Automated Excel generation)

🔑 Key Features

Automated MDM Logic: Prioritizes records based on source system hierarchy and SIM activation recency.

Robust Data Cleaning: Handles inconsistent delimiters (semicolons vs. commas) and varying column headers across platforms.

Business Intelligence: Automatically calculates weekly reporting windows and aggregates metrics (Revenue, SMS, Calls).

Professional Reporting: Generates a formatted Excel Executive Summary for the Marketing Department.
