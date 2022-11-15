# data-engineering-final-project
In this project, our team SWAN develop a data pipeline for ingesting data from the US Department of Health and Human Services (HHS), producing a SQL database, and automatically generating reports.

**Schema.sql**
Includes three table: 
	1. facility_information (hospitcal's information, will not change in the future)
	2. quality_ratings (quality ratings for each hospital from the Centers for Medicare and Medicaid Services, update yearly)
	3. facility_reports (each hospital's facility information, update weekly)

**Load-hhs.py**
This Python code will load HHS data each week and CMS quality data as needed. It helps to handle conversions, cleaning, and reformatting of the data, and insert data into one of three tables.

As the person who upload the dataset, you are required to have valid user ID and password.

When starting processing uploaded dataset, the first step is do data cleaning, if the number is 0 or none, it will update as None. 
For the upload dataset, we will separate it two two parts: new hospital and exist hospital. 
For the new hospital which hospital_pk not in the facility_id,  Python will update the new hospital information in the facility_information table.
If the hospital's information already exist in the facility_information table, Python will automatically insert weekly update to facility_reports table.

If there's any error happened during the time insert dataset, it will print message let people know failed at which row.

If a row is invalid and rejected by Postgres, Python write that row out to a separate CSV file. 

If the dataset updated successfully, Python will report the uumber of rows inserted into facility_information and quality_reports, also the number of rows updated in facility_information
