# ETL_sprint_2.5

ETL-процесс для перекачки данных из Poestgresql в Elasticsearch

## Основная логика работы
Используется три ETL-процесса для загрузки или обновления данных (diff_load.py)
* загрузка изменений в таблице кинопроизведений
* загрузка изменений, связанных с редактированием персон
* загрузка изменений, связанных с редактированием жанров

И один процесс для удаленных записей (check_deleted.py)

### Алгоритм загрузки или обновления кинопроизведений
В storage.json хранится текущее состояние загрузки(loading) и время последней загрузки.
Если параметр loading=False, то:
* выгружаются новые или отредактированные кинопроизведения, при наличии, и производится загрузка в Elasticsearch
* выгружаются все кинопроизведения с отредактированными жанрами или персонами, при наличии, и производится загрузка в Elasticsearch
* меняется статус загрузки
* записывается отметка времени

### Синхронизация удаленных записей

В два множества записываются все id кинопроизведений из postgres и elasticsearch. Вычитанием множеств получаем нужные id, которые и удаляются пачками.


Перед запуском проекта в корневую папку необходимо добавить файл .env с описанием реквизитов доступа.
Формат файла .env
```
DB_ENGINE = django.db.backends.postgresql_psycopg2
DB_NAME = <название базы данных>
POSTGRES_USER = <имя пользователя >
POSTGRES_PASSWORD = <пароль пользователя>
DB_HOST = <имя хоста> # при запуске в docker-compose следует указывать название сервиса - db
DB_PORT = <порт>
```

### Пример заданий cron
```
# загрузка кинопроизведений каждую минуту
* * * * * python <путь до файла>/diff_load.py
# синхронизация удаленных кинопроизведений, раз в сутки
@daily python <путь до файла>/check_deleted.py
```