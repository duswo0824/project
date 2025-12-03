from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

id = 'web_user'
password = 'pass'
host = 'localhost:3306'
db = 'project' # DB에 새로생성 (create database project;)
url = f'mysql+pymysql://{id}:{password}@{host}/{db}'

engine = create_engine(url, pool_size=1)
session = sessionmaker(bind=engine)

def get_conn():
    return session()

# DB 사용시 안쓰는 건데, PANDAS와 DB 연동에서는 사용됨
def get_engine():
    return engine
