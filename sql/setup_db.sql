-- POSTGRES
------------------------------------------------------------------------------------------------------------------------
-- SETUP SOME DATABASE OBJECTS...
------------------------------------------------------------------------------------------------------------------------
create schema food_inspections;
alter schema food_inspections owner to dopyjhogbbiriu;

create schema raw;
alter schema raw owner to dopyjhogbbiriu;

create schema stage;
alter schema stage owner to dopyjhogbbiriu;

set search_path to food_inspections;
show search_path;

create extension cube;
create extension earthdistance;

-- Not sure if I will be using this or not...
-- Token validation
-- create extension pgcrypto;
--
-- create table restaurants.app_settings (
--     setting_name text,
--     setting_value text
-- )

set search_path to raw, stage, food_inspections;
show search_path;

------------------------------------------------------------------------------------------------------------------------
-- SETUP RAW TABLE TO IMPORT DATA INTO...
------------------------------------------------------------------------------------------------------------------------

-- drop table raw.fulton_inspection_violations;

create table raw.fulton_inspection_violations
(
	inspection_id text,
	inspection_item text,
	inspection_type text,
	facility text,
	address text,
	city text,
	state text,
	zipcode text,
	inspection_date text,
	permit_number text,
	score text,
	grade text,
	purpose text,
	risk_type text,
	last_inspection_score text,
	last_inspection_grade text,
	last_inspection_date text,
	prior_inspection_score text,
	prior_inspection_grade text,
	prior_inspection_date text,
	follow_up_needed text,
	follow_up_date text,
	foodborne_illness_risk text,
	inspector_date_time_in text,
	inspector_date_time_out text
);

alter table raw.fulton_inspection_violations owner to dopyjhogbbiriu;

-- drop table food_inspections.fulton_inspections;

------------------------------------------------------------------------------------------------------------------------
-- SETUP FINAL TABLE FOR FULTON COUNTY INSPECTION RESULTS...
------------------------------------------------------------------------------------------------------------------------

-- drop table food_inspections.fulton_inspections;

create table food_inspections.fulton_inspections
(
	inspection_id text constraint fulton_inspections_pkey primary key,
	inspection_date date,
	permit_number text,
	facility text,
	address text,
	city text,
	state text,
	zipcode text,
    latitude numeric(12,7),
    longitude numeric(12,7),
	score int,
	grade text,
	purpose text,
	risk_type text,
	number_violations int,
	last_inspection_score int,
	last_inspection_grade text,
	last_inspection_date date,
	prior_inspection_score int,
	prior_inspection_grade text,
	prior_inspection_date date,
	follow_up_needed boolean,
	follow_up_date date,
	inspector_date_time_in timestamp,
	inspector_date_time_out timestamp,
    created_ymd timestamp default now(),
    updated_ymd timestamp default now()

);

alter table food_inspections.fulton_inspections owner to dopyjhogbbiriu;

------------------------------------------------------------------------------------------------------------------------
-- SETUP VIEW TO PULL ALL INSPECTIONS TOGETHER...
-- CURRENTLY ONLY HAVE FULTON COUNTY BUT WOULD LIKE TO ADD MORE METRO ATL COUNTIES DOWN THE ROAD.
------------------------------------------------------------------------------------------------------------------------

create view food_inspections.vw_inspections as
select
    permit_number,
    facility,
    address,
    city,
    state,
    zipcode,
    inspection_date,
    score,
    latitude,
    longitude
from food_inspections.fulton_inspections;

alter view food_inspections.vw_inspections owner to dopyjhogbbiriu;

------------------------------------------------------------------------------------------------------------------------
-- THE END...
------------------------------------------------------------------------------------------------------------------------
