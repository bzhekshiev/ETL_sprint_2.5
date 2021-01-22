import json
import logging
import os
from typing import List
from urllib.parse import urljoin

import psycopg2
import requests
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from config import *

logger = logging.getLogger()


class ESLoader:
    def __init__(self, url: str):
        self.url = url

    def _get_es_bulk_query(self, rows: List[dict], index_name: str) -> List[str]:
        '''
        Подготавливает bulk-запрос в Elasticsearch
        '''
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps(
                    {'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row)
            ])
        return prepared_query

    def load_to_es(self, records: List[dict], index_name: str):
        '''
        Отправка запроса в ES и разбор ошибок сохранения данных
        '''
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )

        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)


class PostgresSaver:

    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn

    def get_person(self, timestamp):
        cur = self.pg_conn.cursor()
        args = cur.mogrify(timestamp).decode()
        cur.execute(f'''SELECT id
                        FROM content.person
                        WHERE updated_at > '{args}'
                        ORDER BY updated_at
                        LIMIT 100; 
                        ''')
        persons = cur.fetchall()
        return persons

    def get_person_film_work(self, timestamp, persons: List):
        cur = self.pg_conn.cursor()
        SQL = '''SELECT fw.id, fw.updated_at
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                WHERE fw.updated_at > %s AND pfw.person_id IN (%s)
                ORDER BY fw.updated_at
                LIMIT 100; '''

        query = cur.mogrify(SQL, (timestamp, persons))
        res = cur.execute(query).fetchall()
        return res

    def get_movies(self, timestamp):
        cur = self.pg_conn.cursor(cursor_factory=DictCursor)
        args = cur.mogrify(timestamp).decode()

        cur.execute(f'''SELECT
                    fw.id, 
                    fw.title, 
                    fw.description, 
                    fw.rating, 
                    fw.type, 
                    fw.created, 
                    fw.modified, 
                    p.id as person_id,
                    P.FIRST_NAME || ' ' || P.LAST_NAME as person_name,
                    pfw.role,
                    g.name as genre
                FROM public.movies_filmwork fw
                LEFT JOIN public.movies_personfilmwork pfw ON pfw.film_work_id = fw.id
                LEFT JOIN public.movies_person p ON p.id = pfw.person_id
                LEFT JOIN public.movies_genrefilmwork gfw ON gfw.film_work_id = fw.id
                LEFT JOIN public.movies_genre g ON g.id = gfw.genre_id
                WHERE fw.id IN (SELECT FW.ID
                FROM PUBLIC.MOVIES_FILMWORK FW
                LEFT JOIN PUBLIC.MOVIES_PERSONFILMWORK PFW ON PFW.FILM_WORK_ID = FW.ID
                WHERE FW.MODIFIED > '{args}'
                        AND PFW.PERSON_ID IN (SELECT ID
                FROM PUBLIC.MOVIES_PERSON
                WHERE MODIFIED > '{args}'
                ORDER BY MODIFIED
                LIMIT 100
                )
                ORDER BY FW.MODIFIED
                LIMIT 100);''')

        rows = cur.fetchall()
        row_dict = [{k: v for k, v in record.items()} for record in rows]
        return row_dict

    def _transform_row(self, rows: dict) -> dict:
        unique_movies = {}
        roles = ['actor', 'writer', 'director']
        
        for movie in rows:
            movie_id = movie.get('id')
            movie['created'] = str(movie.get('created'))
            movie['modified'] = str(movie.get('modified'))
            if movie_id not in unique_movies.keys():
                unique_movies[movie_id] = movie
                unique_movies[movie_id]['actors_names'] = []
                unique_movies[movie_id]['actors'] = []
                unique_movies[movie_id]['writers_names'] = []
                unique_movies[movie_id]['writers'] = []
                unique_movies[movie_id]['directors'] = []
                unique_movies[movie_id]['directors_names'] = []
                unique_movies[movie_id]['genre'] = []

                if movie.get('genre'):
                    unique_movies[movie_id]['genre'].append(movie.get('genre'))
                for role in roles:
                    if movie.get('role') == role and movie.get('person_name') is not None:
                        unique_movies[movie_id][f'{role}s'].append(
                            {'id': movie.get('person_id'), 'name': movie.get('person_name').strip()})

                        unique_movies[movie_id][f'{role}s_names'].append(
                            movie.get('person_name').strip())

            else:
                destination_record = unique_movies.get(movie_id)
                if movie.get('genre') is not None and movie.get('genre') not in destination_record['genre']:
                    destination_record['genre'].append(movie.get('genre'))
                for role in roles:
                    if movie.get('role') == role and movie.get('person_name') is not None and movie.get('person_name').strip() not in destination_record[f'{role}s_names']:
                        destination_record[f'{role}s'].append(
                            {'id': movie.get('person_id'), 'name': movie.get('person_name').strip()})
                        destination_record[f'{role}s_names'].append(
                            movie.get('person_name').strip())

        result = []

        for val in unique_movies.values():
            del val['person_id']
            del val['person_name']
            del val['role']
            result.append(val)

        return result


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('POSTGRES_USER'),
           'password': os.environ.get('POSTGRES_PASSWORD'),
           'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}

    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        es = ESLoader('http://127.0.0.1:9200/')
        p = PostgresSaver(pg_conn)
        timestamp = '2021-01-20 13:13:21.003762+03'
        movies = p.get_movies(timestamp)
        data = p._transform_row(movies)

        es.load_to_es(data,'test_load')

