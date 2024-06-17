from os import getenv
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

TG_BOT_TOKEN = getenv("TG_BOT_TOKEN")
NAME_LINK = getenv("NAME_LINK")


MONGO_URI = getenv('MONGO_URI')
DB_NAME = getenv('DB_NAME')
COLLECTION_NAME = getenv('COLLECTION_NAME')

WRONG_MESSAGE = 'Невалидный запос. Пример запроса: \n {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
