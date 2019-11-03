-- drop function return_closest_restaurants;

CREATE OR REPLACE FUNCTION return_closest_restaurants(
    IN user_latitude numeric,
    IN user_longitude numeric,
    IN num_results integer)
  RETURNS TABLE(
    restaurant text,
    address text,
    city text,
    state text,
    zipcode text,
    inspection_date date,
    score int,
    latitude numeric,
    longitude numeric,
    distance numeric) AS
$BODY$
DECLARE
    ref refcursor;
  BEGIN

  RETURN QUERY
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
        from restaurants.inspections as a
        order by a.permit_number, a.inspection_date desc
    )
    select *
    from restaurant_scores
    order by distance asc
    limit num_results;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;

 -- NOTE: You need to change the owner to your database login...
ALTER FUNCTION return_closest_restaurants(numeric, numeric, integer)
  OWNER TO osaevtapyrcflq;