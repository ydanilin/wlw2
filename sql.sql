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



SELECT f.id_, f.name FROM firmas f
INNER JOIN cat_per_firm s ON s.firma_id = f.id_
WHERE s.cat_id IN (

SELECT c.id_ FROM job_state j
LEFT JOIN categories c ON c.name_in_url = j.name_in_url
WHERE j.name_in_url = 'tiefdruck')

