from postgres_to_elk_diff_load import ESLoader, PostgresSaver
import config
import psycopg2

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor


def get_movie_ids_from_es():
    es = ESLoader(config.ES_PATH)
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

es = get_movie_ids_from_es()
ps = get_movie_ids_from_psql()

deleted_movies = es.difference(ps))
