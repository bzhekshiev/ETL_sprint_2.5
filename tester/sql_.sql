SELECT id, updated_at
FROM content.person
WHERE updated_at > '<время>'
ORDER BY updated_at
LIMIT 100; 



SELECT fw.id, fw.updated_at
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
WHERE fw.updated_at > '<время>' AND pfw.person_id IN (<id_всех_людей>)
ORDER BY fw.updated_at
LIMIT 100; 



SELECT
    fw.id as fw_id, 
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.created_at, 
    fw.updated_at, 
    pfw.role, 
    p.id, 
    p.full_name,
    g.name
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.id IN (<id_всех_кинопроизводств>); 