
In this project, our team SWAN develop a data pipeline for ingesting data from the US Department of Health and Human Services (HHS), producing a SQL database, and automatically generating reports.

*schema.sql* Includes three table: 
1. facility_information (hospitcal's information, might include new hospitals every update) 
2. quality_ratings (quality ratings for each hospital from the Centers for Medicare and Medicaid Services, update yearly or every half-year) 
3. facility_reports (each hospital's facility information, update weekly)

*load_hhs.py* This file will load HHS data each week and CMS quality data as needed. It helps to handle conversions, cleaning, and reformatting of the data, then inserting and updating data into two of three tables. The code example for using load_hhs.py is below:

 - python load-hhs.py 2022-01-04-hhs-data.csv

As the person who upload the dataset, you are required to have valid user ID and password.

If there's any error happened during the time inserting into dataset, it will print out a message let people know which row has failed.

If a row is invalid and rejected by Postgres, Python will include all the error rows and print out to a separate CSV file.

If the dataset updated successfully, Python will report the number of rows inserted into facility_information and quality_reports, also the number of rows updated in facility_information. Note that the update count is not accurate since the Python program will iterate through the entire table and update all rows that are presented in the input file (e.g. it will update all previously input and some mismatch hospital from the quality_reports table), we only expected:

- Number of rows inserted into and updated in facility information = Number of rows inserted into facility_reports

*load_quality.py* Similar to load_hhs, update and insert new hospital's rating related information to facility_information table and quality_ratings table.

*loadinghelper.py* This file will help data loading, it includes four functions:
1. Check the negative input variable
2. Split the geocoded hospital address
3. Process the null input variable
4. Get the existing hospital ids for future processing
