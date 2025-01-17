import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import config
from diff_load import ESLoader, PostgresSaver

es = ESLoader(config.ES_PATH)


def get_movie_ids_from_es(es: ESLoader):

    size = config.ES_SIZE
    total_rows = es.get_all_data(size=10, from_=0).get(
        'hits').get('total').get('value')

    es_ids = set()
    for i in range(total_rows//size+1):
        es_data = es.get_all_data(size=size, from_=(i*size))
        for item in es_data.get('hits').get('hits'):
            es_ids.add(item.get('_id'))
    return es_ids


def get_movie_ids_from_psql():
    with psycopg2.connect(**config.dsl, cursor_factory=DictCursor) as pg_conn:
        p = PostgresSaver(pg_conn)
        ids = set(p.get_all_movies())
        return ids


def delete_rows(elk_movies, ps_movies):
    """Удаляет кинопроизведения из ELK, которые были удалены в Postgres"""
    deleted_movies = elk_movies.difference(ps_movies)
    es.load_to_es(deleted_movies, config.INDEX_NAME, 'DELETE')


if __name__ == "__main__":
    elk_movies = get_movie_ids_from_es(es)
    ps_movies = get_movie_ids_from_psql()
    delete_rows(elk_movies, ps_movies)
