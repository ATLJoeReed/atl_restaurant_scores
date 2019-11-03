-- truncate table raw.inspections;

-- load data from file

select *
from raw.inspections;

-- truncate table restaurants.inspections;

insert into restaurants.inspections
    (inspection_id, facility, address, city, state, zipcode, inspection_date,
    permit_number, score, grade, purpose, risk_type, last_inspection_score,
    last_inspection_grade, last_inspection_date, prior_inspection_score,
    prior_inspection_grade, prior_inspection_date, follow_up_needed, follow_up_date,
    inspector_date_time_in, inspector_date_time_out)
select
    inspection_id, facility, address, city, state, zipcode, inspection_date::date,
    permit_number, score::int, grade, purpose, risk_type, last_inspection_score,
    last_inspection_grade, last_inspection_date::timestamp, prior_inspection_score,
    prior_inspection_grade, prior_inspection_date::timestamp, follow_up_needed::boolean,
    follow_up_date::timestamp, inspector_date_time_in::timestamp, inspector_date_time_out::timestamp
from raw.inspections
on conflict on constraint inspections_pkey
do
    update
        set facility = excluded.facility,
            address = excluded.address,
            city = excluded.city,
            state = excluded.state,
            zipcode = excluded.zipcode,
            inspection_date = excluded.inspection_date::date,
            permit_number = excluded.permit_number,
            score = excluded.score::int,
            grade = excluded.grade,
            purpose = excluded.purpose,
            risk_type = excluded.risk_type,
            last_inspection_score = excluded.last_inspection_score,
            last_inspection_grade = excluded.last_inspection_grade,
            last_inspection_date = excluded.last_inspection_date::timestamp,
            prior_inspection_score = excluded.prior_inspection_score,
            prior_inspection_grade = excluded.prior_inspection_grade,
            prior_inspection_date = excluded.prior_inspection_date::timestamp,
            follow_up_needed = excluded.follow_up_needed::boolean,
            follow_up_date = excluded.follow_up_date::timestamp,
            inspector_date_time_in = excluded.inspector_date_time_in::timestamp,
            inspector_date_time_out = excluded.inspector_date_time_out::timestamp,
            updated_ymd = now();

select *
from restaurants.inspections
where latitude is null;

select distinct
    permit_number, address, city, state, zipcode
from restaurants.inspections
where latitude is null
    or longitude is null
order by zipcode;

select * from return_closest_restaurants(33.6880178, -84.42331870000001, 10);

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
    from restaurants.inspections
    order by permit_number, inspection_date desc
)
select *
from restaurant_scores
order by distance asc
limit 200;

