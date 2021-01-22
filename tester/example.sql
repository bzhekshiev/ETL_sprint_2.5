SELECT ID,
	MODIFIED
FROM PUBLIC.MOVIES_PERSON
WHERE MODIFIED > '2020-12-26 15:36:33.196826+00'
ORDER BY MODIFIED
LIMIT 100;


SELECT FW.ID,
	FW.MODIFIED
FROM PUBLIC.MOVIES_FILMWORK FW
LEFT JOIN PUBLIC.MOVIES_PERSONFILMWORK PFW ON PFW.FILM_WORK_ID = FW.ID
WHERE FW.MODIFIED > '2020-12-26 15:36:33.196826+00'
				AND PFW.PERSON_ID IN (SELECT ID
FROM PUBLIC.MOVIES_PERSON
WHERE MODIFIED > '2020-12-26 15:36:33.196826+00'
ORDER BY MODIFIED
LIMIT 100
)
ORDER BY FW.MODIFIED
LIMIT 100;

-- {
--    "id":"tt0076759",
--    "genre":[
--       "Action",
--       "Adventure",
--       "Fantasy",
--       "Sci-Fi"
--    ],
--    "writers":[
--       {
--          "id":"0b60f2f348adc2f668a9a090165e24f3d3a7cf5a",
--          "name":"George Lucas"
--       }
--    ],
--    "actors":[
--       {
--          "id":"1",
--          "name":"Mark Hamill"
--       },
--       {
--          "id":"2",
--          "name":"Harrison Ford"
--       },
--       {
--          "id":"3",
--          "name":"Carrie Fisher"
--       },
--       {
--          "id":"4",
--          "name":"Peter Cushing"
--       }
--    ],
--    "actors_names":[
--       "Mark Hamill",
--       "Harrison Ford",
--       "Carrie Fisher",
--       "Peter Cushing"
--    ],
--    "writers_names":[
--       "George Lucas"
--    ],
--    "imdb_rating":8.6,
--    "title":"Star Wars: Episode IV - A New Hope",
--    "director":[
--       "George Lucas"
--    ],
--    "description":"The Imperial Forces, under orders from cruel Darth Vader, hold Princess Leia hostage in their efforts to quell the rebellion against the Galactic Empire. Luke Skywalker and Han Solo, captain of the Millennium Falcon, work together with the companionable droid duo R2-D2 and C-3PO to rescue the beautiful princess, help the Rebel Alliance and restore freedom and justice to the Galaxy."
-- }


SELECT
    fw.id as fw_id, 
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.created, 
    fw.modified, 
	array_agg_mult(p.first_name || ' ' || p.last_name) actors,


	g.id,
    g.name
FROM public.movies_filmwork fw
LEFT JOIN public.movies_personfilmwork pfw ON pfw.film_work_id = fw.id
LEFT JOIN public.movies_person p ON p.id = pfw.person_id
LEFT JOIN public.movies_genrefilmwork gfw ON gfw.film_work_id = fw.id
LEFT JOIN public.movies_genre g ON g.id = gfw.genre_id
WHERE fw.id IN (SELECT FW.ID
FROM PUBLIC.MOVIES_FILMWORK FW
LEFT JOIN PUBLIC.MOVIES_PERSONFILMWORK PFW ON PFW.FILM_WORK_ID = FW.ID
WHERE FW.MODIFIED > '2020-12-26 15:36:33.196826+00'
				AND PFW.PERSON_ID IN (SELECT ID
FROM PUBLIC.MOVIES_PERSON
WHERE MODIFIED > '2020-12-26 15:36:33.196826+00'
ORDER BY MODIFIED
LIMIT 100
)
ORDER BY FW.MODIFIED
LIMIT 100)
group by     fw.id ,
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.created, 
    fw.modified, 



	g.id,
    g.name
; 