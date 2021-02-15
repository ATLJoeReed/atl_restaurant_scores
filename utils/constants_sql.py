#!/usr/bin/python3.9
# -*- coding: utf-8 -*-
FETCH_SCORES = """
select
    restaurant,
    to_char(inspection_date, 'MM/DD/YY') as inspection_date,
    score
from return_closest_restaurants(%(latitude)s, %(longitude)s, %(num_scores)s);
"""

INSERT_FULTON_INSPECTIONS_SQL = """
insert into raw.fulton_inspections (inspection_id, facility, 
    address, city, state, zipcode, inspection_date, permit_number, score, 
    grade, purpose, risk_type, last_inspection_score, last_inspection_grade, 
    last_inspection_date, prior_inspection_score, prior_inspection_grade, 
    prior_inspection_date, follow_up_needed, follow_up_date, 
    inspector_date_time_in, inspector_date_time_out) 
values (%(inspection_id)s, %(facility)s, %(address)s, %(city)s, %(state)s, 
    %(zipcode)s, %(date)s, %(permit_number)s, %(score)s, %(grade)s, 
    %(purpose)s, %(risk_type)s, %(last_score)s, %(last_grade)s, %(last_date)s,
    %(prior_score)s, %(prior_grade)s, %(prior_date)s, %(follow_up_needed)s, 
    %(follow_up_date)s, %(date_time_in)s, %(date_time_out)s)
""" # noqa

INSERT_FULTON_VIOLATIONS_SQL = """
insert into raw.fulton_violations (inspection_id, inspection_item, 
    inspection_type, facility, address, city, state, zipcode, inspection_date, 
    permit_number, score, grade, purpose, risk_type, last_inspection_score, 
    last_inspection_grade, last_inspection_date, prior_inspection_score, 
    prior_inspection_grade, prior_inspection_date, follow_up_needed, follow_up_date, 
    foodborne_illness_risk, inspector_date_time_in, inspector_date_time_out) 
values (%(inspection_id)s, %(item)s, %(type)s, %(facility)s, %(address)s, %(city)s, 
    %(state)s, %(zipcode)s, %(date)s, %(permit_number)s, %(score)s, %(grade)s, 
    %(purpose)s, %(risk_type)s, %(last_score)s, %(last_grade)s, %(last_date)s,
    %(prior_score)s, %(prior_grade)s, %(prior_date)s, %(follow_up_needed)s, 
    %(follow_up_date)s, %(foodborne_illness_risk)s, %(date_time_in)s, 
    %(date_time_out)s)
""" # noqa

MERGE_FULTON_INSPECTIONS_SQL = """
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
""" # noqa

MERGE_FULTON_VIOLATIONS_SQL = """
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
""" # noqa

SELECT_GEOCODING_SQL = """
select distinct
    permit_number, address, city, state, zipcode
from food_inspections.fulton_inspections
where latitude is null
    or longitude is null
order by zipcode;
"""

UPDATE_GEOCODING_SQL = """
update food_inspections.fulton_inspections
    set latitude = %(lat)s,
        longitude = %(lng)s
where permit_number = %(permit_number)s
    and address = %(address)s
    and city = %(city)s
    and state = %(state)s
    and zipcode = %(zipcode)s;
"""
