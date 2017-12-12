--- Note that I'm commenting / modifying the SQL code that starts on line 64 of 2016's demo script (where it says: ### Example Queries:)

-- Build schemas and load the csv data for the tables fact_country, fact_population, dim_student.
CREATE TABLE fact_country (
	name string, -- Country name
	code string, -- Country abreviation
	currency_name string,
	is_independent string, 
	capital string, 
	continent string, 
	languages string
) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '/home/vagrant/demo_script/country.csv' OVERWRITE INTO TABLE fact_country;

CREATE TABLE fact_population (
	name string, -- Country name
	code string, -- Country abreviation
	year int, -- Year of existence for the country
	value int -- Population of the country
) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '/home/vagrant/demo_script/population.csv' OVERWRITE INTO TABLE fact_population;

CREATE TABLE dim_student (
	id int, -- Student ID
	name string,  
	gender string, 
	age int, 
	country string -- Country of origin
) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '/home/vagrant/demo_script/student.csv' OVERWRITE INTO TABLE dim_student;


---------- QUERY 1 ----------:
---- PROBLEM: What are the populations of all the countries that are not independent in 2010?
-- Join fact_population with fact_country to get information about the current year for the country and whether it's independent. 
-- Filter the countries that exist in 2010 but are not independent.
-- Order by population (in ascending order)
select c.name as country, p.value as population, c.is_independent 
from fact_population as p
join fact_country as c
on (c.code = p.code)
where p.year = 2010 and is_independent  <>  'Yes'
order by p.value;

---------- QUERY 2 ----------:
---- PROBLEM: How many male students come from countries that are not independent?
-- Join dim_student with fact_country to get info about the student and whether their home country is independent
-- Filter males from countries that were not independent
select count(c.name) as male_students
from dim_student as s
join fact_country as c
on (s.country = c.name)
where  s.gender = 'male' AND c.is_independent  <>  'Yes';


---------- QUERY 3 ----------:
---- PROBLEM: In 2010, for each of the Asian countries (home to at least 1 of the students), what percentage of its population attends this school?
-- Join fact_population with fact_country to determine both continent and population of the country
-- Filter out entries from year 2010 (to get 2010 populations)
-- Filter out entries for countries from Asia
-- Group together entries of the same country
-- Order by poulation (in ascending order)
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


---------- QUERY 4 ----------:
---- NOTE: Run all of QUERY 4 at once because dim_student is mutated
---- PROBLEM: There's been an influx of European foreign exchange students. What's the % change in Europeans?
declare @initEuro INT
set @initEuro = (
	select count(*)
	from dim_student as s
	join fact_country as c
	on (s.country = c.name)
	where c.continent = 'EU'
);

-- Add in the new European students
-- NOTE: We can also populate these into a new table using the newstudent_pradhith.csv and insert from that
INSERT INTO dim_student VALUES (1000,'JOSEF','male',12,'Norway');
INSERT INTO dim_student VALUES (1001,'ANDY','male',27,'Lithuania');
INSERT INTO dim_student VALUES (1002,'EILEEN','female',71,'Turkey');
INSERT INTO dim_student VALUES (1003,'TISHA','female',46,'Germany');
INSERT INTO dim_student VALUES (1004,'NESTOR','male',28,'Iceland');
INSERT INTO dim_student VALUES (1005,'CRYSTAL','female',15,'Netherlands');
INSERT INTO dim_student VALUES (1006,'DUNCAN','male',85,'Denmark');
INSERT INTO dim_student VALUES (1007,'HARRIS','male',53,'Republic of Moldova');
INSERT INTO dim_student VALUES (1008,'KRISTIE','female',55,'Latvia');
INSERT INTO dim_student VALUES (1009,'ULYSSES','male',38,'Switzerland');
INSERT INTO dim_student VALUES (1010,'MAURA','female',48,'Azerbaijan');
INSERT INTO dim_student VALUES (1011,'KORY','male',57,'Iceland');
INSERT INTO dim_student VALUES (1012,'CYRUS','male',92,'Georgia');
INSERT INTO dim_student VALUES (1013,'SIMON','male',87,'Romania');
INSERT INTO dim_student VALUES (1014,'LANA','female',75,'Finland');
INSERT INTO dim_student VALUES (1015,'JESSIE','female',44,'San Marino');
INSERT INTO dim_student VALUES (1016,'SYLVESTER','male',49,'Latvia');
INSERT INTO dim_student VALUES (1017,'JACOB','male',24,'Croatia');
INSERT INTO dim_student VALUES (1018,'KRISTINE','female',48,'Poland');
INSERT INTO dim_student VALUES (1019,'NADIA','female',66,'Liechtenstein');
INSERT INTO dim_student VALUES (1020,'WILLIS','male',78,'Sweden');
INSERT INTO dim_student VALUES (1021,'SARAH','female',29,'Italy');
INSERT INTO dim_student VALUES (1022,'HERSCHEL','male',65,'Germany');
INSERT INTO dim_student VALUES (1023,'DONA','female',27,'Italy');
INSERT INTO dim_student VALUES (1024,'CLARE','female',39,'Ukraine');

declare @finalEuro INT
set @finalEuro = (
	select count(*)
	from dim_student as s
	join fact_country as c
	on (s.country = c.name)
	where c.continent = 'EU'
);
-- compute the percentage change
select 100. * (@finalEuro - @initEuro) / @initEuro as percent_change