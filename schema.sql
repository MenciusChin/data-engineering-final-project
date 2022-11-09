/* The script for designing the tables for project */

/*
Three entities were designed in the schema:
    - facility_information 
    - quality_ratings REFERENCES facility_information(facility_id)
    - facility_reports REFERENCES facility_information(facility_id)
It was designed based on the assumption that:
    - Unique facility/hosipital
    - Hospital one-to-many ratings (Yearly)
    - Hospital one-to-may reports (Weekly)
*/

/* This table stores all information of a hospital */
CREATE TABLE facility_information (
    facility_id TEXT PRIMARY KEY,
    facility_name TEXT NOT NULL,
    facility_type TEXT NOT NULL,
    emergency_service BOOLEAN NOT NULL,
    -- We include all geographic information in the table avoid excessive JOINs
    geocoded_hospital_address TEXT NOT NULL,
    address TEXT NOT NULL, 
    city TEXT NOT NULL,
    state_abbrev CHAR(2) NOT NULL,
    state_name TEXT NOT NULL,
    -- Assume zipcode are either 00000 or 00000-0000
    zipcode VARCHAR(10) NOT NULL,
    -- fipscode are 00000
    fipscode CHAR(5) NOT NULL,
    county TEXT NOT NULL
)

/* We assume there will be many quality ratings for one hospital, 
   And we would like to keeo track of the ratings. */
CREATE TABLE quality_ratings (
    rating_id SERIAL PRIMARY KEY,
    rating_year DATE CHECK (rating_year <= NOW()),
    -- We stored the different ratings in this column
    rating TEXT NOT NULL,
    -- So we references to the facility_information table here
    facility_id TEXT REFERENCES facility_information(facility_id)
)

/* We assume there will be many reports for one hospital, 
   And we would like to keeo track of the reports. */
CREATE TABLE facility_reports (
    report_id SERIAL PRIMARY KEY
    report_date DATE CHECK (report_date <= NOW()),
    hospital_pk TEXT REFERENCES facility_information(facility_id),
    hospital_name TEXT NOT NULL,
    -- The total number of hospital beds available each week, broken down into adult and pediatric (children) beds
    total_adult_hospital_beds NUMERIC NOT NULL
        CHECK (total_adult_hospital_beds >= 0),
    total_pediatric_hospital_beds NUMERIC NOT NULL
        CHECK (total_pediatric_hospital_beds >= 0),
    -- The number of hospital beds that are in use each week
    total_adult_hospital_beds_occupied NUMERIC NOT NULL
        CHECK (total_adult_hospital_beds_occupied >= 0),
    total_pediatric_hospital_beds_occupied NUMERIC NOT NULL
        CHECK (total_pediatric_hospital_beds_occupied >= 0),
    -- The number of ICU (intensive care unit) beds available and the number in use
    total_icu_beds NUMERIC NOT NULL CHECK (total_icu_beds >= 0),
        CHECK (total_icu_beds <= total_adult_hospital_beds + total_pediatric_hospital_beds),
    total_icu_beds_occupied NUMERIC NOT NULL CHECK (total_icu_beds_occupied >= 0),
        CHECK (total_icu_beds_occupied <= total_adult_hospital_beds_occupied +
               total_pediatric_hospital_beds_occupied)
    -- The number of patients hospitalized who have confirmed COVID
    inpatient_beds_occupied_covid NUMERIC NOT NULL CHECK (inpatient_beds_occupied_covid >= 0),
        CHECK (inpatient_beds_occupied_covid <= total_adult_hospital_beds_occupied +
               total_pediatric_hospital_beds_occupied)
    -- The number of adult ICU patients who have confirmed COVID
    adult_icu_patients_confirmed_covid NUMERIC NOT NULL 
        CHECK (adult_icu_patients_confirmed_covid <= inpatient_beds_occupied_covid),
        CHECK (adult_icu_patients_confirmed_covid >= 0),
)