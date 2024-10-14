# Databricks notebook source
# MAGIC %fs ls abfss://tdhpgslandint@tdhpgsnpdlandin01.dfs.core.windows.net/SureshTEST/

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dev_greater_sydney_division_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema midt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_greater_sydney_division_bronze.midt.people

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dev_greater_sydney_division_bronze.midt.people (
# MAGIC   FullName VARCHAR(250),
# MAGIC   Company VARCHAR(100),
# MAGIC   BirthDate VARCHAR(100),
# MAGIC   Salary FLOAT,
# MAGIC   Address VARCHAR(255),
# MAGIC   Post VARCHAR(100),
# MAGIC   Phone VARCHAR(100),
# MAGIC   Mobile VARCHAR(100),
# MAGIC   Email VARCHAR(255),
# MAGIC   Age INT,
# MAGIC   SalaryBucket VARCHAR(10),
# MAGIC   State VARCHAR(100),
# MAGIC   Suburb VARCHAR(100))
# MAGIC USING delta
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '1',
# MAGIC   'delta.minWriterVersion' = '2');
# MAGIC  

# COMMAND ----------

import csv
from datetime import datetime
import math
import pandas as pd
from pyspark.sql.types import FloatType
from pyspark.sql.functions import to_json, col

columns = ['FirstName', 'LastName', 'Company', 'BirthDate', 'Salary', 'Address', 'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email']

# Function to fomat the birht date
def format_birthdate(birth_date):
    try:
        birth_date = datetime.strptime(birth_date, '%d%m%Y')
        return birth_date
    except (TypeError, ValueError):
        return None

# This funciton calculate age based on reference Mar 1st, 2024
def calculate_age(birth_date, reference_date=datetime(2024, 3, 1)):
    delta = reference_date - birth_date
    return math.floor(delta.days / 365)

# This function SalaryBucket to categorize the employees based on their salary
def categorize_salary(salary):
    salary = float(salary)
    if salary < 50000:
        return 'A'
    elif 50000 <= salary <= 100000:
        return 'B'
    else:
        return 'C'

def transform(row):
    birth_date = format_birthdate(row['BirthDate'])
    if birth_date is not None:
        row['BirthDate'] = birth_date.strftime('%d/%m/%Y')
        row['Age'] = calculate_age(birth_date)
    #remove empty spaces
    row['FirstName'] = row['FirstName'].strip()
    row['LastName'] = row['LastName'].strip()
    
    # Merging firstname and lastname
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
    return row

# Main ETL process
file_path = '/Volumes/dev_greater_sydney_division_bronze/nag_test/test/member-data.csv'

print("Begin of ETL process...")
print(f"Reading Data from the File: {file_path}")

# Read data file
df = pd.read_csv(file_path, header=None, delimiter='|', dtype={'BirthDate':str, 'Mobile':str, 'Phone':str}, names=columns)

# call Transform fucniton
transformed_data = df.apply(transform, axis=1)

# Drop FirstName, LastName columns
select_data = transformed_data.drop(['FirstName', 'LastName'], axis=1)

# Converting datatypes
spark_df = spark.createDataFrame(select_data)
spark_df = spark_df.withColumn("Salary", spark_df["Salary"].cast(FloatType()))
spark_df = spark_df.withColumn("Address", to_json(col("Address")))
spark_df = spark_df.withColumn("Post", col("Post").cast("string"))
spark_df = spark_df.withColumn("Age", col("Age").cast("int"))


# Write DataFrame to the table
#spark_df.write.mode("overwrite").insertInto("dev_greater_sydney_division_bronze.midt.people")
spark_df.write.mode("overwrite").saveAsTable("dev_greater_sydney_division_bronze.midt.people")
display(spark_df)



# COMMAND ----------

spark_df
