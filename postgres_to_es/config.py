import os
from pathlib import Path

from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

BASE_DIR = Path(__file__).resolve(strict=True).parent
CHUNK_SIZE = 100

ES_PATH = 'http://127.0.0.1:9200/'
ES_SIZE = 15
INDEX_MOVIE = 'movie_test'
INDEX_GENRE= 'genre'
INDEX_PERSON = 'person'


dsl = {'dbname': os.environ.get('DB_NAME'),
       'user': os.environ.get('POSTGRES_USER'),
       'password': os.environ.get('POSTGRES_PASSWORD'),
       'host': os.environ.get('DB_HOST'),
       'port': os.environ.get('DB_PORT')}
