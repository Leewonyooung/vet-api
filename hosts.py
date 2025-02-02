import pymysql
import os, json
import boto3
import redis.asyncio as redis
from firebase_admin import credentials, initialize_app

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
REGION = os.getenv('AWS_REGION')
VET_DB = os.getenv('VET_DB')
VET_USER = os.getenv('VET_DB_USER')
VET_PASSWORD = os.getenv('VET_DB_PASSWORD')
VET_TABLE = os.getenv('VET_DB_TABLE')
VET_PORT = os.getenv('VET_PORT')
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")



s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION
)


firebase_key_json = os.getenv("VET_FIREBASE_KEY")
if not firebase_key_json:
    raise ValueError("VET_FIREBASE_KEY environment variable is not set")

# JSON 문자열을 Python 딕셔너리로 변환
firebase_key = json.loads(firebase_key_json)

# Firebase 초기화
cred = credentials.Certificate(firebase_key)
print(cred)
initialize_app(cred)


redis_client = None
async def get_redis_connection():
    global redis_client
    if not redis_client:
        try:
            print("Initializing Redis connection pool...")
            # Redis 연결 풀 생성
            connection_pool = redis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                max_connections=10,  # 연결 풀 크기 설정
                decode_responses=True  # 문자열 디코딩 활성화
            )
            redis_client = redis.Redis(connection_pool=connection_pool)
            # 연결 테스트
            await redis_client.ping()
            print("Redis connection pool established.")
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            redis_client = None
            raise e
    return redis_client

async def close_redis_connection():
    global redis_client
    if redis_client:
        print("Closing Redis connection pool...")
        await redis_client.close()
        redis_client = None
        print("Redis connection pool closed.")


def connect():
    conn = pymysql.connect(
        host=VET_DB,
        user=VET_USER,
        password=VET_PASSWORD,
        charset='utf8',
        db=VET_TABLE,
        port=int(VET_PORT)
    )
    return conn