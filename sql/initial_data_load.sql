-- truncate table food_inspections.fulton_inspections;
-- truncate table food_inspections.fulton_violations;
-- truncate table raw.fulton_inspections;
-- truncate table raw.fulton_violations;

-- load inspections data from file into raw table...
select *
from raw.fulton_inspections;

-- load violations data from file into raw table...
select *
from raw.fulton_violations;

-- Little cleanup on inspections...
select *
from raw.fulton_inspections
where score = 'P' or last_inspection_score = 'P' or prior_inspection_score = 'P'

update raw.fulton_inspections
    set last_inspection_score = null
where last_inspection_score = 'P';
-- 4 rows...

update raw.fulton_inspections
    set prior_inspection_score = null
where prior_inspection_score = 'P';
-- 5 rows



-- Load up inspections...merge statement...
insert into food_inspections.fulton_inspections
    (inspection_id, inspection_date, permit_number, facility, address, city, state, zipcode,
    score, grade, purpose, risk_type, last_inspection_score, last_inspection_grade,
    last_inspection_date, prior_inspection_score, prior_inspection_grade, prior_inspection_date,
    follow_up_needed, follow_up_date, inspector_date_time_in, inspector_date_time_out)
select
    inspection_id,
    inspection_date::date as inspection_date,
    permit_number,
    facility,
    address,
    city,
    state,
    zipcode,
    score::int,
    grade,
    purpose,
    risk_type,
    nullif(last_inspection_score, '')::int,
    last_inspection_grade,
    last_inspection_date::date as last_inspection_date,
    nullif(prior_inspection_score, '')::int,
    prior_inspection_grade,
    prior_inspection_date::date as prior_inspection_date,
    follow_up_needed::boolean as follow_up_needed,
    follow_up_date::date as follow_up_date,
    inspector_date_time_in::timestamp,
    inspector_date_time_out::timestamp
from raw.fulton_inspections
on conflict on constraint fulton_inspections_pkey
do
    update
        set inspection_date = excluded.inspection_date::date,
            permit_number = excluded.permit_number,
            facility = excluded.facility,
            address = excluded.address,
            city = excluded.city,
            state = excluded.state,
            zipcode = excluded.zipcode,
            score = excluded.score::int,
            grade = excluded.grade,
            purpose = excluded.purpose,
            risk_type = excluded.risk_type,
            last_inspection_score = excluded.last_inspection_score::int,
            last_inspection_grade = excluded.last_inspection_grade,
            last_inspection_date = excluded.last_inspection_date::date,
            prior_inspection_score = excluded.prior_inspection_score::int,
            prior_inspection_grade = excluded.prior_inspection_grade,
            prior_inspection_date = excluded.prior_inspection_date::date,
            follow_up_needed = excluded.follow_up_needed::boolean,
            follow_up_date = excluded.follow_up_date::date,
            inspector_date_time_in = excluded.inspector_date_time_in::timestamp,
            inspector_date_time_out = excluded.inspector_date_time_out::timestamp,
            updated_ymd = now();

select *
from food_inspections.fulton_inspections;

-- Load up the violations...merge statement...
insert into food_inspections.fulton_violations
    (inspection_id, inspection_item, inspection_type, foodborne_illness_risk, num_item_violations)
select
    inspection_id,
    inspection_item,
    coalesce(inspection_type, '-') as inspection_type,
    foodborne_illness_risk::boolean,
    count(*) as num_item_violations
from raw.fulton_violations
group by inspection_id, inspection_item, coalesce(inspection_type, '-'), foodborne_illness_risk
on conflict on constraint fulton_violations_pkey
do
    update
        set inspection_id = excluded.inspection_id,
            inspection_item = excluded.inspection_item,
            inspection_type = excluded.inspection_type,
            foodborne_illness_risk = excluded.foodborne_illness_risk,
            num_item_violations = excluded.num_item_violations,
            updated_ymd = now();

select *
from food_inspections.fulton_violations
where num_item_violations > 1;

------------------------------------------------------------------------------------------------------------------------
-- RETAG LATITUDE/LONGITUDE...
------------------------------------------------------------------------------------------------------------------------

set search_path to stage, food_inspections;
show search_path;

update food_inspections.fulton_inspections
    set latitude = null,
        longitude = null;

-- Load initial geocoded records...

select *
from stage.initial_geocodes;

update fulton_inspections
    set latitude = initial_geocodes.latitude,
        longitude = initial_geocodes.longitude
from initial_geocodes
where fulton_inspections.permit_number = initial_geocodes.permit_number
    and fulton_inspections.address = initial_geocodes.address
    and fulton_inspections.city = initial_geocodes.city
    and fulton_inspections.state = initial_geocodes.state
    and fulton_inspections.zipcode = initial_geocodes.zipcode;

select distinct
    permit_number, address, city, state, zipcode
from food_inspections.fulton_inspections
where latitude is null
    or longitude is null
order by zipcode;

-- This built the initial_geocoding.csv file..
select distinct
    permit_number,
    facility,
    address,
    city,
    state,
    zipcode::text,
    latitude,
    longitude
from food_inspections.fulton_inspections
where latitude is not null
    and longitude is not null
order by permit_number;
