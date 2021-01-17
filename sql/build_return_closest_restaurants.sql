-- drop function food_inspections.return_closest_restaurants;

create or replace function food_inspections.return_closest_restaurants(
    in user_latitude numeric,
    in user_longitude numeric,
    in num_results integer)
  returns table(
    restaurant text,
    address text,
    city text,
    state text,
    zipcode text,
    inspection_date date,
    score int,
    latitude numeric,
    longitude numeric,
    distance numeric) as
$body$
declare
    ref refcursor;
  begin

  return query
    with restaurant_scores as
    (
        select distinct on (a.permit_number)
            a.facility as restaurant,
            a.address,
            a.city,
            a.state,
            a.zipcode,
            a.inspection_date,
            a.score,
            a.latitude,
            a.longitude,
            cast(earth_distance(ll_to_earth(user_latitude , user_longitude),
                     ll_to_earth(a.latitude, a.longitude)) * .0006213712 as numeric(10,2)) as "distance"
        from food_inspections.vw_inspections as a
        order by a.permit_number, a.inspection_date desc
    )
    select *
    from restaurant_scores
    order by distance asc
    limit num_results;

end;
$body$
  language plpgsql volatile
  cost 100
  rows 1000;

 -- note: you need to change the owner to your database login...
alter function food_inspections.return_closest_restaurants(numeric, numeric, integer)
  owner to dopyjhogbbiriu;
