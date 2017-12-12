---- These queries are largely taken from RDFS 2016's Demo Script.

-- Build schemas and load the csv data for the tables fact_country, fact_population, dim_student.
CREATE TABLE fact_country (
	name string, -- Country name
	code string, -- Country abreviation
	currency_name string,
	is_independent string, 
	capital string, 
	continent string, 
	languages string
);
LOAD DATA LOCAL INPATH '/home/vagrant/apache-hive-2.1.0-bin/mockdata/country.csv' OVERWRITE INTO TABLE fact_country;

CREATE TABLE fact_population (
	name string, -- Country name
	code string, -- Country abreviation
	year int, -- Year of existence for the country
	value int -- Population of the country
);
LOAD DATA LOCAL INPATH '/home/vagrant/apache-hive-2.1.0-bin/mockdata/population.csv' OVERWRITE INTO TABLE fact_population;

CREATE TABLE dim_student (
	id int, -- Student ID
	name string,  
	gender string, 
	age int, 
	country string -- Country of origin
);
LOAD DATA LOCAL INPATH '/home/vagrant/apache-hive-2.1.0-bin/mockdata/student.csv' OVERWRITE INTO TABLE dim_student;


---------- QUERY 1 ----------:
---- PROBLEM: What are the populations of all the countries that are not independent in 2010?

select c.name as country, p.value as population, c.is_independent 
from fact_population as p
-- Join fact_population with fact_country to get information about the current year for the country and whether it's independent. 
join fact_country as c
on (c.code = p.code)
-- Filter the countries that exist in 2010 but are not independent.
where p.year = 2010 and is_independent  <>  'Yes'
-- Order by population (in ascending order)
order by p.value;

---------- QUERY 2 ----------:
---- PROBLEM: How many male students come from countries that are not independent?

select count(c.name) as male_students
-- Join dim_student with fact_country to get info about the student and whether their home country is independent
from dim_student as s
join fact_country as c
on (s.country = c.name)
-- Filter males from countries that were not independent
where  s.gender = 'male' AND c.is_independent  <>  'Yes';


---------- QUERY 3 ----------:
---- PROBLEM: In 2010, for each of the Asian countries (home to at least 1 of the students), what percentage of its population attends this school?
select count(s.id) as num_students, s.country, t.value as population, 100. * count(s.id)/t.value as percentage_at_school
from dim_student as s 
join
-- Join fact_population with fact_country to determine both continent and population of the country
(select c.name, c.continent, p.value
from fact_population as p
join fact_country as c
on (c.code = p.code)
-- Filter out entries from year 2010 (to get 2010 populations)
where  p.year=2010)
as t
on (s.country = t.name)
-- Filter out entries for countries from Asia
where t.continent = 'AS' 
-- Group together entries of the same country
group by s.country, t.value
-- Order by poulation (in ascending order)
order by t.value;