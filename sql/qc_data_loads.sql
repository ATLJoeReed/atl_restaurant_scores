-- POSTGRES
set search_path to raw, stage, food_inspections;
show search_path;

------------------------------------------------------------------------------------------------------------------------
-- CHECK RAW TABLES...
------------------------------------------------------------------------------------------------------------------------
select *
from raw.fulton_inspections;

select *
from raw.fulton_violations;

------------------------------------------------------------------------------------------------------------------------
-- CHECKOUT SOME OF THE LATEST RECORDS...
------------------------------------------------------------------------------------------------------------------------
select *
from food_inspections.fulton_inspections
order by created_ymd desc
limit 5000;

select *
from food_inspections.fulton_inspections
order by updated_ymd desc
limit 5000;

select *
from food_inspections.fulton_inspections
order by inspection_date desc
limit 5000;

select *
from food_inspections.fulton_violations
order by created_ymd desc
limit 5000;

------------------------------------------------------------------------------------------------------------------------
-- CHECK FOR ORPHAN RECORDS...
------------------------------------------------------------------------------------------------------------------------
select a.*
from food_inspections.fulton_inspections as a
    left join food_inspections.fulton_violations as b
        on trim(a.inspection_id) = trim(b.inspection_id)
where b.inspection_id is null
order by a.inspection_date desc;

-- Appears that we have many inspections without any associated violations...
-- 2021-02-14 - 5,538 inspections with no associated violation records.

select *
from food_inspections.fulton_violations
where inspection_id = '30854'
order by inspection_id desc;

select a.*
from food_inspections.fulton_violations as a
    left join food_inspections.fulton_inspections as b
        on trim(a.inspection_id) = trim(b.inspection_id)
where b.inspection_id is null
order by a.inspection_id desc;

-- Appears all violations are associated with an inspection records...

------------------------------------------------------------------------------------------------------------------------
-- CHECK FOR RECORDS NOT ASSIGNED A LATITUDE OR LONGITUDE - THESE ARE RECORDS GOOGLE GEOCODING PROCESS COULD NOT
-- ASSOCIATE WITH A LATITUDE/LONGITUDE. NEED TO MANUALLY LOOK THESE UP.
------------------------------------------------------------------------------------------------------------------------
select distinct
    facility, address, city, state, zipcode, latitude, longitude
from food_inspections.fulton_inspections
where latitude is null
    or longitude is null;

------------------------------------------------------------------------------------------------------------------------
-- CHECK CLOSEST RESTAURANT PROCESS...
------------------------------------------------------------------------------------------------------------------------
select *
from food_inspections.vw_inspections;

select *
from food_inspections.return_closest_restaurants(33.6880178, -84.42331870000001, 10);

--Home: 33.6880178, -84.42331870000001
with restaurant_scores as
(
    select distinct on (permit_number)
        facility as restaurant,
        address,
        city,
        state,
        zipcode,
        inspection_date,
        score,
        latitude,
        longitude,
        cast(earth_distance(ll_to_earth(33.6880178 , -84.42331870000001),
                 ll_to_earth(latitude, longitude)) * .0006213712 as numeric(10,2)) as "distance"
    from food_inspections.vw_inspections
    order by permit_number, inspection_date desc
)
select *
from restaurant_scores
order by distance asc
limit 50;

-- Needs a new inspection...
select *
from food_inspections.fulton_inspections
where address ilike '834 Cleveland Ave%'

------------------------------------------------------------------------------------------------------------------------
-- LOOK AT TUNING CLOSEST RESTAURANT PROCESS...
------------------------------------------------------------------------------------------------------------------------
-- Didn't find any good index uses to speed this up.
-- Further research is needed...

select *
from food_inspections.return_closest_restaurants(33.6880178, -84.42331870000001, 5);

explain analyse
select distinct on (permit_number)
    facility as restaurant,
    address,
    city,
    state,
    zipcode,
    inspection_date,
    score,
    latitude,
    longitude,
    cast(earth_distance(ll_to_earth(33.6880178 , -84.42331870000001),
             ll_to_earth(latitude, longitude)) * .0006213712 as numeric(10,2)) as "distance"
from food_inspections.fulton_inspections
order by permit_number, inspection_date desc;

--Home: 33.6880178, -84.42331870000001
explain analyse
with restaurant_scores as
(
    select distinct on (permit_number)
        facility as restaurant,
        address,
        city,
        state,
        zipcode,
        inspection_date,
        score,
        latitude,
        longitude,
        cast(earth_distance(ll_to_earth(33.6880178 , -84.42331870000001),
                 ll_to_earth(latitude, longitude)) * .0006213712 as numeric(10,2)) as "distance"
    from food_inspections.vw_inspections
    order by permit_number, inspection_date desc
)
select *
from restaurant_scores
order by distance asc
limit 50;

explain analyze
select distinct on (permit_number)
    facility as restaurant,
    address,
    city,
    state,
    zipcode,
    inspection_date,
    score,
    latitude,
    longitude,
    cast(earth_distance(ll_to_earth(33.6880178 , -84.42331870000001),
             ll_to_earth(latitude, longitude)) * .0006213712 as numeric(10,2)) as "distance"
from food_inspections.vw_inspections
order by permit_number, inspection_date desc;

select
    relname as tablename,
    seq_scan as totalseqscan,
    case
        when seq_scan-idx_scan > 0
            then 'missing index found'
        else 'missing index not found'
    end as missingindex,
    pg_size_pretty(pg_relation_size(relname::regclass)) as tablesize,
    idx_scan as totalindexscan
from pg_stat_all_tables
where schemaname='food_inspections'
    and pg_relation_size(relname::regclass)>100000;

-- Unused indexes
select
    indexrelid::regclass as index,
    relid::regclass as table_name,
    'drop index ' || indexrelid::regclass || ';' as drop_statement
from pg_stat_user_indexes
    inner join pg_index using (indexrelid)
where idx_scan = 0
    and indisunique is false;

-- Create some indexes to try and speed up process to fetch closest restaurants...
-- Initial time without indexes: ~1.25 seconds
create index fulton_permit_no_inspection_date_idx on food_inspections.fulton_inspections (permit_number, inspection_date desc);
create index fulton_lat_long_idx on food_inspections.fulton_inspections (latitude, longitude);

create index fulton_inspections_permit_no_idx on food_inspections.fulton_inspections (permit_number);
create index fulton_inspections_inspection_date_idx on food_inspections.fulton_inspections (inspection_date desc);

drop index fulton_permit_no_inspection_date_idx;
drop index fulton_inspections_inspection_date_idx;
drop index fulton_lat_long_idx;
drop index fulton_inspections_permit_no_idx;
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------