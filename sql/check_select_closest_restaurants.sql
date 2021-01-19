select *
from food_inspections.vw_inspections;

select distinct
    permit_number, address, city, state, zipcode
from food_inspections.vw_inspections
where latitude is null
    or longitude is null
order by zipcode;

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
