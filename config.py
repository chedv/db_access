import os


db_config = dict(host=os.environ['DB_HOST'],
                 port=os.environ['DB_PORT'],
                 dbname=os.environ['DB_NAME'],
                 user=os.environ['DB_USER'],
                 password=os.environ['DB_PASSWORD'])
