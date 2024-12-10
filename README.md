# Data Pipeline Project

## Project Overview

This project is a data pipeline designed to handle ETL (Extract, Transform, Load) processes for e-commerce data. It reads raw data from local files, processes and cleans the data, and loads it into a PostgreSQL database. The pipeline is structured to be modular, using Python classes and scripts to maintain clear separation of responsibilities.

## Project Structure

```plaintext
data-pipeline/
├── .env.example                # Example environment configuration file
├── README.md                   # Project description and usage guide
├── requirements.txt            # List of project dependencies
├── data/ 
│   ├── local/                  # Data folder for input and output
│   │   ├── raw/                
│   │   │   └── raw_e-commerce_data.csv  # Raw data file
│   │   └── cleaned/            # Directory for cleaned data (optional)
├── src/                        # Source code folder
│   ├── __init__.py             # Module initializer
│   ├── data_io_manager.py      # Data reading and writing operations
│   ├── data_cleaner.py         # Data cleaning
│   └── db_connections.py       # Database connection classes
└── scripts/                    # Helper scripts for running the pipeline
    └── etl_pipeline.py         # Script to start the ETL process
```

## Prerequisites

Before running the project, ensure the following are installed:

- Python 3.x
- PostgreSQL database
- Python packages listed in `requirements.txt`

## Setup and Installation

Follow these steps to set up and install the project:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
   ```
2. **Create and Activate a Virtual Environment** (Optional but recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   .\venv\Scripts\activate   # On Windows
   ```
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ``` 
3. **Set up the** .env **file**:

   Copy the .env.example to .env and update the database and file paths as needed.

## Usage
Run the etl_pipeline.py script to start the ETL process:
   ```bash
   python3 scripts/etl_pipeline.py
   ```
This script will:

1. **Extract**: Read raw data from the CSV file.
2. **Transform**: Clean and process the data by handling missing values, capping outliers, removing duplicates, and eliminating highly correlated features.
3. **Load**: Save the cleaned data into a PostgreSQL database table.

**Data Reading and Writing**
- LocalDataHandler: Handles reading and writing local data files (e.g., CSV, JSON, Parquet).
- PostgresDataHandler: Manages reading and writing data to the PostgreSQL database.

**Data Cleaning**

- The DataCleaner class performs data cleaning tasks such as:
- Filling missing values with median or mode.
- Capping outliers within specified percentiles.
- Removing duplicate rows.
- Removing highly correlated features.

## Project Details
- **Technologies Used**:
  - Python
  - Pandas
  - SQLAlchemy
  - PostgreSQL
  - Logging
  - dotenv 

- **File Descriptions**:

  - **data_io_manager.py**: Provides methods for reading data from files and writing data to files.
  - **data_cleaner.py**: Contains methods to clean data and handle transformations.
  - **db_connections.py**: Establishes connections to PostgreSQL and handles database interactions.
  - **etl_pipeline.py**: Entry point for executing the ETL process.
