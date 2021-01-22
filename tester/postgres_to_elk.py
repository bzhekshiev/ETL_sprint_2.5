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
from state import JsonFileStorage, State
from utils import current_time

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

    def get_obj(self,obj_name, timestamp):
        cur = self.pg_conn.cursor()
        args = cur.mogrify(timestamp).decode()
        cur.execute(f'''SELECT id
                        FROM public.{obj_name}
                        WHERE modified > '{args}'
                        ORDER BY modified
                        LIMIT {CHUNK_SIZE}; 
                        ''')
        obj = cur.fetchall()
        return [r[0] for r in obj]

    def get_obj_film_work(self,obj,timestamp):
        cur = self.pg_conn.cursor()
        buf = "','".join(self.get_obj(f'movies_{obj}',timestamp))
        cur.execute(f'''SELECT fw.id
                FROM public.movies_filmwork fw
                LEFT JOIN public.movies_{obj}filmwork pfw ON pfw.film_work_id = fw.id
                WHERE fw.modified > '{timestamp}' AND pfw.{obj}_id IN ('{buf}')
                ORDER BY fw.modified
                LIMIT 100; ''')

        res = cur.fetchall()
        return [r[0] for r in res]
   

    def get_movies_by_obj(self, buf, timestamp):
        cur = self.pg_conn.cursor()
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
                WHERE fw.id IN ('{buf}');''')
        rows = cur.fetchall()
        row_dict = [{k: v for k, v in record.items()} for record in rows]
        return row_dict

    def get_total_movies(self, timestamp):
        persons_film_work = "','".join(self.get_obj_film_work('person',timestamp))
        genre_film_work = "','".join(self.get_obj_film_work('genre',timestamp))
        total = self.get_movies_by_obj(persons_film_work, timestamp)
        # ТУТ ОШИБКА!!!
        # print(len(self.get_movies_by_obj(genre_film_work, timestamp)[0]))
        # total.append(self.get_movies_by_obj(genre_film_work, timestamp))
        return total

# 9c226388-1ab3-4160-bcf2-727cf720112a
    def _transform_row(self, rows: list) -> dict:
        unique_movies = {}
        roles = ['actor', 'writer', 'director']
        for movie in rows:
            # print(movie)
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



        storage = JsonFileStorage(os.path.join(BASE_DIR, 'storage.json'))
        state = State(storage)
        # print(state.get_state('last_work_time'))
        # state.set_state('last_work_time',current_time())
        movies = p.get_total_movies(timestamp)
        data = p._transform_row(movies)
        print(len(data))

        # es.load_to_es(data,'test_load')
