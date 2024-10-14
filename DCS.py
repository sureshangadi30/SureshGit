# Databricks notebook source
# MAGIC %fs ls abfss://tdhpgslandint@tdhpgsnpdlandin01.dfs.core.windows.net/SureshTEST/

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dev_greater_sydney_division_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema midt

# COMMAND ----------

import csv
from datetime import datetime
import math

# Function to read data from CSV
def read_data(file_path):
    schema = ['FirstName', 'LastName', 'Company', 'BirthDate', 'Salary', 'Address', 'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email']
    data = []
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='|', fieldnames=schema)
        next(reader)  # Skip header
        for row in reader:
            data.append(row)
    return data

# Preprocess and format birthdate to handle non-standard formats like '3021981'
def preprocess_birthdate(birth_date):
    if len(birth_date) == 7:  # Handle dates like '3021981' (DDMMYYYY but missing leading zero)
        birth_date = '0' + birth_date  # Add leading zero for day
    elif len(birth_date) == 8:  # Assume it's in DDMMYYYY format
        return birth_date
    else:
        return None  # Return None if the date format is completely unrecognized

# Format birthdate and handle different formats
def format_birthdate(birth_date):
    birth_date = preprocess_birthdate(birth_date)
    if birth_date is None:
        return None  # If preprocessing failed, return None
    
    try:
        # Try parsing as DDMMYYYY
        return datetime.strptime(birth_date, '%d%m%Y')
    except ValueError:
        return None  # Return None if parsing fails

# Calculate age based on birthdate and a reference date
def calculate_age(birth_date, reference_date=datetime(2024, 3, 1)):
    delta = reference_date - birth_date
    return math.floor(delta.days / 365)

# Categorize salary into buckets
def categorize_salary(salary):
    salary = float(salary)
    if salary < 50000:
        return 'A'
    elif 50000 <= salary <= 100000:
        return 'B'
    else:
        return 'C'

# Transform data by cleaning and adding calculated fields
def transform_data(data):
    for row in data:
        # Format birth date and clean names
        birth_date = format_birthdate(row['BirthDate'])
        
        if birth_date is None:
            print(f"Warning: Invalid date format for record: {row['BirthDate']}. Skipping...")
            row['BirthDate'] = "Invalid Date"  # Assign a placeholder for invalid dates
        else:
            row['BirthDate'] = birth_date.strftime('%d/%m/%Y')
            row['Age'] = calculate_age(birth_date)
        
        # Clean names
        row['FirstName'] = row['FirstName'].strip()
        row['LastName'] = row['LastName'].strip()
        
        # Merge first and last names
        row['FullName'] = f"{row['FirstName']} {row['LastName']}"
        
        # Categorize salary into buckets
        row['SalaryBucket'] = categorize_salary(row['Salary'])
        
        # Create nested Address field
        row['Address'] = {
            'Street': row['Address'],
            'Suburb': row['Suburb'],
            'State': row['State'],
            'PostCode': row['Post']
        }
        
        # Remove unnecessary columns
        #del row['FirstName'], row['LastName'], row['Suburb'], row['State'], row['Post']
    return data

# Function to display the schema of the transformed data
def print_schema(data_transformed):
    if data_transformed:
        print("Schema of transformed data:")
        for key in data_transformed[0].keys():
            print(f"- {key}")
    else:
        print("No data available to display schema.")

# Main ETL process
if __name__ == "__main__": 
    file_path = '/Volumes/dev_greater_sydney_division_bronze/nag_test/test/member-data.csv'
    
    print("Starting ETL process...")
    print(f"Reading Data from the File: {file_path}")
    
    # Read data from the file
    data = read_data(file_path)
    
    # Transform data
    data_transformed = transform_data(data)
    display(data_transformed)
    #print_schema(data_transformed)

    # Display transformed data
    #for record in data_transformed:
    #    display(record)
    


