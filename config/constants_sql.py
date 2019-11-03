#!/usr/bin/python3.7
# -*- coding: utf-8 -*-
CHECK_TOKEN = """
select exists(
    select 1
    from restaurants.app_settings
    where setting_name = %(token_type)s
        AND setting_value = CRYPT(%(token)s, setting_value)
);
"""

FETCH_SCORES = """
select restaurant, address, city, state, zipcode, score, distance::text
from return_closest_restaurants(%(latitude)s, %(longitude)s, %(num_scores)s);
""" # noqa

INSERT_INSPECTIONS_SQL = """
insert into raw.inspections (inspection_id, facility, address, city, 
    state, zipcode, inspection_date, permit_number, score, grade, purpose, 
    risk_type, last_inspection_score, last_inspection_grade, last_inspection_date,
    prior_inspection_score, prior_inspection_grade, prior_inspection_date, 
    follow_up_needed, follow_up_date, inspector_date_time_in, inspector_date_time_out) 
values (%(inspection_id)s, %(facility)s, %(address)s, %(city)s, 
    %(state)s, %(zipcode)s, %(date)s, %(permit_number)s, %(score)s, %(grade)s, %(purpose)s, 
    %(risk_type)s, %(last_score)s, %(last_grade)s, %(last_date)s,
    %(prior_score)s, %(prior_grade)s, %(prior_date)s, 
    %(follow_up_needed)s, %(follow_up_date)s, %(date_time_in)s, %(date_time_out)s)
""" # noqa

MERGE_INSPECTIONS_SQL = """
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
""" # noqa

SELECT_GEOCODING_SQL = """
select distinct
    permit_number, address, city, state, zipcode
from restaurants.inspections
where latitude is null
    or longitude is null
order by zipcode;
"""

UPDATE_GEOCODING_SQL = """
update restaurants.inspections
    set latitude = %(lat)s,
        longitude = %(lng)s
where permit_number = %(permit_number)s
    and address = %(address)s
    and city = %(city)s
    and state = %(state)s
    and zipcode = %(zipcode)s;
"""
