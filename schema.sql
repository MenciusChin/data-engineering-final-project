/* The script for designing the tables */

CREATE TABLE facility_information (
    facility_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    facility_type TEXT NOT NULL,
    emergency_service BOOLEAN NOT NULL
)

/* We assume there will be many quality ratings for one hospital, 
   And we would like to keeo track of the ratings. */
CREATE TABLE quality_ratings (
    rating_id SERIAL PRIMARY KEY,
    rating_date DATE CHECK (rating_date < NOW()),
    -- We stored the different ratings in this column
    rating TEXT NOT NULL,
    -- So we references to the facility_information table here
    facility_id REFERENCES facility_information(facility_id)
)