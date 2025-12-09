-- Quickly take a look at the data
SELECT * FROM road_safety_events
LIMIT 10;

-- check if there are any null values
SELECT * FROM road_safety_events
WHERE occurrencedate IS NULL or occurrencedetail IS NULL 
	or municipality IS NULL or roadsafetyoccurrencetype IS NULL;
-- no null values

SELECT DISTINCT roadsafetyoccurrencetype 
FROM road_safety_events;
-- 9 different types

SELECT DISTINCT locationcode
FROM road_safety_events;
-- 9 different locations

SELECT DISTINCT municipality
FROM road_safety_events;
-- 9 different municipalities

SELECT DISTINCT involvedrugoralcohol
FROM road_safety_events;
-- 4 categories: alcohol, drugs, alcohol and drugs, neither

-- number of accidents by region
SELECT 
	Municipality,
	count(UniqueIdentifier) as numAccidents
FROM road_safety_events
GROUP BY Municipality
ORDER BY numAccidents desc;
-- Vaughan ranked first, Markham second, Richmond Hill third

-- number of accidents by location
SELECT 
	locationcode,
	count(UniqueIdentifier) as numAccidents,
	ROUND(count(UniqueIdentifier) * 100 / SUM(COUNT(UniqueIdentifier)) OVER(), 2) as percentage
FROM road_safety_events
GROUP BY locationcode
ORDER BY numAccidents desc;
-- Outdoor makes up 98%, residence around 1%, business around 1%

-- number of accidents by type
SELECT 
	roadsafetyoccurrencetype,
	count(UniqueIdentifier) as numAccidents,
	ROUND(count(UniqueIdentifier) * 100 / SUM(COUNT(UniqueIdentifier)) OVER(), 2) as percentage
FROM road_safety_events
GROUP BY roadsafetyoccurrencetype
ORDER BY numAccidents desc
-- Collisions make up 90%, careless driving second, driving disqualified third

-- number of accidents with alcohol and/or drug involved
SELECT 
	involvedrugoralcohol,
	count(UniqueIdentifier) as numAccidents,
	ROUND(count(UniqueIdentifier) * 100 / SUM(COUNT(UniqueIdentifier)) OVER(), 2) as percentage
FROM road_safety_events
GROUP BY involvedrugoralcohol
ORDER BY numAccidents desc;
-- 94% involved no alcohol, 5% only involved alcohol, drugs only 0.4%, alc and drugs 0.22%

	
	.