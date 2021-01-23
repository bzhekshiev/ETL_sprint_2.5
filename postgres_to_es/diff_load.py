import json
import logging
import os
from time import sleep
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

    def _get_es_bulk_query(
            self, rows: List[dict],
            index_name: str) -> List[str]:
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

    def _prepared_for_delete(
            self, rows: List[str],
            index_name: str) -> List[str]:
        '''
        Подготавливает bulk-запрос для удаления записей в Elasticsearch 
        '''
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps(
                    {'delete': {'_index': index_name, '_id': row}}),
            ])
        return prepared_query

    def load_to_es(
            self, records: List[dict],
            index_name: str, method: str = None):
        '''
        Отправка запроса в ES и разбор ошибок сохранения данных
        :method позволяет использовать другой метод подготовки данных при удалении
        '''
        if method == 'DELETE':
            prepared_query = self._prepared_for_delete(records, index_name)
        else:
            prepared_query = self._get_es_bulk_query(records, index_name)

        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query,
            headers={'Content-Type': 'application/x-ndjson'}
        )

        json_response = json.loads(response.content.decode())
        if json_response.get('items'):
            for item in json_response.get('items'):
                error_message = item['index'].get('error')
                if error_message:
                    logger.error(error_message)

    def get_all_data(self, from_: str, size: int) -> dict:
        """Получить все записи из ElasticSearch
        :size - кол-во выгружаемых записей
        :from_ - отметка, с которой читать записи
        """
        req = requests.get(
            urljoin(
                self.url,
                f'{INDEX_NAME}/_search/?sort={{created:ASC}}&size={size}&from={from_}'))
        return req.json()


class PostgresSaver:

    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn

    def get_obj(self, obj_name: str, timestamp) -> List[str]:
        """Получение списка измененных записей жанров или персон"""
        cur = self.pg_conn.cursor()
        args = cur.mogrify(timestamp).decode()
        cur.execute(f'''SELECT id
                        FROM public.{obj_name}
                        WHERE modified > '{args}'
                        ORDER BY modified
                        LIMIT {CHUNK_SIZE}; 
                        ''')
        obj = cur.fetchall()

        if obj != []:
            return [r[0] for r in obj]
        return None

    def get_obj_film_work(self, obj: str, timestamp) -> List[str]:
        """Получение списка id фильмов с изменившимися жанрами или персонами"""
        cur = self.pg_conn.cursor()
        movies = self.get_obj(f'movies_{obj}', timestamp)
        if movies is not None:
            buf = "','".join(movies)
            cur.execute(f'''SELECT fw.id
                    FROM public.movies_filmwork fw
                    LEFT JOIN public.movies_{obj}filmwork pfw ON pfw.film_work_id = fw.id
                    WHERE  pfw.{obj}_id IN ('{buf}')
                    ORDER BY fw.modified
                    LIMIT {CHUNK_SIZE}; ''')

            res = cur.fetchall()
            return [r[0] for r in res]
        return None

    def get_movies_by_obj(self, buf: List[str], timestamp) -> List[dict]:
        """Получение данных по кинопроизведениям с изменившимися жанрами или персонами"""
        if buf is not None and buf != []:
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
        return None

    def get_changed_movies(self, timestamp) -> List[str]:
        """Получение данных изменившихся кинопроизведений"""
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
                WHERE fw.modified >'{timestamp}'
                LIMIT {CHUNK_SIZE};''')
        rows = cur.fetchall()
        if rows != []:
            row_dict = [{k: v for k, v in record.items()} for record in rows]
            return row_dict
        return None

    def get_all_movies(self):
        """Получение всех кинопроизведений"""
        cur = self.pg_conn.cursor()
        cur.execute(f'''SELECT fw.id FROM public.movies_filmwork fw''')
        rows = cur.fetchall()
        if rows != []:
            return [r[0] for r in rows]
        return None

    def get_total_movies(self, timestamp):
        result = {'persons': None, 'genres': None}
        persons_buf = self.get_obj_film_work('person', timestamp)
        if persons_buf is not None:
            persons_film_work = "','".join(persons_buf)
            persons = self.get_movies_by_obj(persons_film_work, timestamp)
            result['persons'] = persons

        genres_buf = self.get_obj_film_work('genre', timestamp)

        if genres_buf is not None:
            genre_film_work = "','".join(genres_buf)
            genres = self.get_movies_by_obj(genre_film_work, timestamp)
            result['genres'] = genres

        return result

    def _transform_row(self, rows: list) -> dict:
        """Подготавливает данных к виду, необходимому для загрузки в Elasticsearch"""
        unique_movies = {}
        roles = ['actor', 'writer', 'director']
        if rows is not None:
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
                        unique_movies[movie_id]['genre'].append(
                            movie.get('genre'))
                    for role in roles:
                        if movie.get('role') == role and movie.get(
                                'person_name') is not None:
                            unique_movies[movie_id][f'{role}s'].append(
                                {'id': movie.get('person_id'),
                                 'name': movie.get('person_name').strip()})

                            unique_movies[movie_id][f'{role}s_names'].append(
                                movie.get('person_name').strip())

                else:
                    destination_record = unique_movies.get(movie_id)
                    if movie.get('genre') is not None and movie.get('genre') not in destination_record['genre']:
                        destination_record['genre'].append(movie.get('genre'))
                    for role in roles:
                        if movie.get('role') == role and movie.get(
                                'person_name') is not None and movie.get(
                                'person_name').strip() not in destination_record[
                                f'{role}s_names']:
                            destination_record[f'{role}s'].append({
                                'id': movie.get('person_id'),
                                'name': movie.get('person_name').strip()})
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
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        es = ESLoader(ES_PATH)
        p = PostgresSaver(pg_conn)

        storage = JsonFileStorage(os.path.join(BASE_DIR, 'storage.json'))
        state = State(storage)

        timestamp = state.get_state('last_work_time')

        loading = state.get_state('')
        if not loading:
            state.set_state('loading', True)

            change_movies = p.get_changed_movies(timestamp)
            data = p._transform_row(change_movies)
            es.load_to_es(data, INDEX_NAME)

            for value in p.get_total_movies(timestamp).values():
                data = p._transform_row(value)
                es.load_to_es(data, INDEX_NAME)

            state.set_state('loading', False)
            state.set_state('last_work_time', current_time())
