-- dev_greater_sydney_division_bronze.midt.people definition

CREATE TABLE dev_greater_sydney_division_bronze.midt.people (
  FullName VARCHAR(250),
  Company VARCHAR(100),
  BirthDate VARCHAR(100),
  Salary FLOAT,
  Address VARCHAR(255),
  Post VARCHAR(100),
  Phone VARCHAR(100),
  Mobile VARCHAR(100),
  Email VARCHAR(255),
  Age INT,
  SalaryBucket VARCHAR(10),
  State VARCHAR(100),
  Suburb VARCHAR(100))
USING delta
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2');
