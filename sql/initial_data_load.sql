-- truncate table food_inspections.fulton_inspections;

-- load data from file

select *
from raw.fulton_inspection_violations;

insert into food_inspections.fulton_inspections
    (inspection_id, inspection_date, permit_number, facility, address, city, state, zipcode,
    score, grade, purpose, risk_type, number_violations, last_inspection_score, last_inspection_grade,
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
    count(*) as number_violations,
    last_inspection_score::int,
    last_inspection_grade,
    last_inspection_date::date as last_inspection_date,
    prior_inspection_score::int,
    prior_inspection_grade,
    prior_inspection_date::date as last_inspection_date,
    follow_up_needed::boolean as follow_up_needed,
    follow_up_date::date as follow_up_date,
    inspector_date_time_in::timestamp,
    inspector_date_time_out::timestamp
from raw.fulton_inspection_violations
where inspection_date::date >= '2018-01-01'
group by inspection_id, inspection_date::date, permit_number, facility, address, city, state, zipcode, score::int,
    grade, purpose, risk_type, last_inspection_score::text, last_inspection_grade, last_inspection_date::date,
    prior_inspection_score::text, prior_inspection_grade, prior_inspection_date::date, follow_up_needed::boolean,
    follow_up_date::date, inspector_date_time_in::timestamp, inspector_date_time_out::timestamp
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
            number_violations = excluded.number_violations,
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
