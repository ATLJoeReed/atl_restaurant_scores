-- drop table raw.inspections;

create table raw.inspections
(
	inspection_id text,
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
	inspector_date_time_in text,
	inspector_date_time_out text
);

alter table raw.inspections owner to osaevtapyrcflq;

create schema restaurants;
alter schema restaurants owner to osaevtapyrcflq;

-- drop table restaurants.inspections;

create table restaurants.inspections
(
	inspection_id text constraint inspections_pkey primary key,
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
	last_inspection_score text,
	last_inspection_grade text,
	last_inspection_date timestamp,
	prior_inspection_score text,
	prior_inspection_grade text,
	prior_inspection_date timestamp,
	follow_up_needed boolean,
	follow_up_date timestamp,
	inspector_date_time_in timestamp,
	inspector_date_time_out timestamp,
    created_ymd timestamp default now(),
    updated_ymd timestamp default now()

);

alter table restaurants.inspections owner to osaevtapyrcflq;

create extension cube;
create extension earthdistance;

select *
from raw.inspections;

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
from restaurants.inspections;