-- base reset (DANGER!!!)
DELETE FROM cat_per_firm;
DELETE FROM categories;
DELETE FROM firmas;
UPDATE job_state SET items_reported = 0,
                     items_stored = 0,
                     last_page = -1,
                     page_seen = '';


-- true ids for categories:
SELECT j.name_in_url, c.id_ FROM job_state j
LEFT JOIN categories c ON c.name_in_url = j.name_in_url 
-- https://www.w3schools.com/sql/sql_join.asp
-- joins s1 and s2 (SELECT <fields> FROM s1 JOIN s2 ON ...):
-- inner join: s1 intersects s2;
-- left join: s1 full + intersections from s2;
-- right join: s1 intersects s2 + s2 full (not supported);
-- full outer join: s1 union s2 (not supported)


-- count amount of firms in each job_state category
SELECT c.caption, COUNT(s.firma_id) AS firmas FROM cat_per_firm s
INNER JOIN firmas f ON s.firma_id = f.id_
INNER JOIN categories c ON s.cat_id = c.id_
WHERE s.cat_id IN (
	SELECT c.id_ FROM job_state j
	INNER JOIN categories c ON c.name_in_url = j.name_in_url)
GROUP BY c.caption

-- join four tables
SELECT * FROM
job_state j INNER JOIN categories c ON j.name_in_url = c.name_in_url
            INNER JOIN cat_per_firm s ON c.id_ = s.cat_id            
            INNER JOIN firmas f ON s.firma_id = f.id_            
ORDER BY j.cat_id


-- based on job_state categories, no duplicates
SELECT f.source,
       f.timestamp,
       group_concat(c.caption, ";") AS category,
       f.id_ AS firmaId,
       f.name AS name,       
       f.full_addr,       
       f.street,       
       f.building,       
       f.zip,       
       f.city,       
       f.phone,       
       f.email,       
       f.site,       
       f.delivery,   
       f.certificates,       
       f.about,       
       f.key_people,       
       f.common_person
FROM
job_state j INNER JOIN categories c ON j.name_in_url = c.name_in_url
            INNER JOIN cat_per_firm s ON c.id_ = s.cat_id            
            INNER JOIN firmas f ON s.firma_id = f.id_            
GROUP BY f.name
ORDER BY j.cat_id


-- amounts based on job_state categories, no duplicates
SELECT category, count(firmaId) FROM (
SELECT f.source,
       f.timestamp,       
       j.cat_id AS cid,
       c.caption AS category,
       f.id_ AS firmaId,
       f.name AS name       
       
FROM
job_state j INNER JOIN categories c ON j.name_in_url = c.name_in_url
            INNER JOIN cat_per_firm s ON c.id_ = s.cat_id            
            INNER JOIN firmas f ON s.firma_id = f.id_            
GROUP BY f.name
ORDER BY j.cat_id)

GROUP BY category
ORDER BY cid


-- group_concat example
SELECT f.name, group_concat((c.caption || " Contact:" || s.contact_person), ";") AS det
FROM
firmas f INNER JOIN cat_per_firm s ON f.id_ = s.firma_id
         INNER JOIN categories c ON s.cat_id = c.id_         
WHERE f.id_ = 1713082


-- a eto pizdets bilattt !!!! *****************************************
SELECT f.source,
(SELECT count(firmaId) AS uniq_firmas FROM (
SELECT f.source,
       f.timestamp,       
       j.cat_id AS cid,
       c.caption AS category,
       f.id_ AS firmaId,
       f.name AS name       
       
FROM
job_state j INNER JOIN categories c ON j.name_in_url = c.name_in_url
            INNER JOIN cat_per_firm s ON c.id_ = s.cat_id            
            INNER JOIN firmas f ON s.firma_id = f.id_            
GROUP BY f.name
ORDER BY j.cat_id)

GROUP BY category
ORDER BY cid) AS total_firms,
       f.timestamp,
       c.caption AS category,
       f.id_ AS firmaId,
       f.name AS name,
       f.full_addr,       
       f.street,       
       f.building,       
       f.zip,       
       f.city,       
       f.phone,       
       f.email,       
       f.site,       
       f.delivery,   
       f.certificates,       
       f.about,       
       f.key_people,       
       f.common_person
FROM
job_state j INNER JOIN categories c ON j.name_in_url = c.name_in_url
            INNER JOIN cat_per_firm s ON c.id_ = s.cat_id            
            INNER JOIN firmas f ON s.firma_id = f.id_            
GROUP BY f.name
ORDER BY j.cat_id
--**************************************************************************









SELECT c.caption, s.firma_id, f.name FROM cat_per_firm s
INNER JOIN firmas f ON s.firma_id = f.id_
INNER JOIN categories c ON s.cat_id = c.id_
WHERE s.cat_id IN (

SELECT c.id_ FROM job_state j
INNER JOIN categories c ON c.name_in_url = j.name_in_url)
GROUP BY c.caption


-- listing on firms per specified job_state category
SELECT f.id_, f.name FROM firmas f
INNER JOIN cat_per_firm s ON s.firma_id = f.id_
WHERE s.cat_id IN (

SELECT c.id_ FROM job_state j
LEFT JOIN categories c ON c.name_in_url = j.name_in_url
WHERE j.name_in_url = 'spezialdruckereien')

-- this one?
-- https://stackoverflow.com/questions/11230725/merge-data-in-two-row-into-one
-- https://stackoverflow.com/questions/7867397/sqlite-merging-rows-into-single-row-if-they-share-a-column

