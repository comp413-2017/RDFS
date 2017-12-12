CREATE TABLE dim_student (
	id int,
	name string,  
	gender string, 
	age int, 
	country string
) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '/home/vagrant/demo_script/student.csv' OVERWRITE INTO TABLE dim_student;

CREATE TABLE fact_population (
	name string,
	code string,
	year int,
	value int
) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '/home/vagrant/demo_script/population.csv' OVERWRITE INTO TABLE fact_population;

select count(d.id)
from dim_student as d
join fact_population as p
on (p.name = d.country)
where d.gender = 'male';

CREATE TABLE fact_country (
	name string,
	code string,
	currency_name string,
	is_independent string, 
	capital string, 
	continent string, 
	languages string
) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '/home/vagrant/demo_script/country.csv' OVERWRITE INTO TABLE fact_country;

select c.name as country, p.value as population, c.is_independent 
from fact_population as p
join fact_country as c
on (c.code = p.code)
where p.year = 2010 and is_independent  <>  'Yes'
order by population;

select count(s.id)
from dim_student as s
join fact_country as c
on (s.country = c.name)
where  s.gender = 'male' AND c.is_independent  <>  'Yes';

select count(s.id) as num_students, s.country, t.value as population, 100. * count(s.id)/t.value as percentage_at_school
from dim_student as s 
join
(select c.name, c.continent, p.value
from fact_population as p
join fact_country as c
on (c.code = p.code)
where  p.year=2010)
as t
on (s.country = t.name)
where t.continent = 'AS' 
group by s.country, t.value
order by t.value;
