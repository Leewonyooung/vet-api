"""
author: 정섭
Description: mypage에서 사용되는 user edit, query with Redis caching
Fixed: 
Usage: 
"""

from fastapi import APIRouter, File, UploadFile, HTTPException
import os
import hosts
from fastapi.responses import StreamingResponse
import io
from botocore.exceptions import ClientError, NoCredentialsError
import auth
import json

mypage_router = APIRouter()

UPLOAD_FOLDER = 'uploads'

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def generate_cache_key(endpoint: str, params: dict):
    return f"{endpoint}:{json.dumps(params, sort_keys=True)}"

async def get_cached_or_fetch(cache_key, fetch_func):
    redis_client = await hosts.get_redis_connection()
    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)
    except Exception as e:
        print(f"Redis get error: {e}")

    data = await fetch_func()
    try:
        await redis_client.set(cache_key, json.dumps(data), ex=3600)
    except Exception as e:
        print(f"Redis set error: {e}")
    return data

@mypage_router.get('/{id}')
async def select_mypage(id: str):
    cache_key = generate_cache_key("select_mypage", {"id": id})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = 'SELECT * FROM user WHERE id=%s'
            curs.execute(sql, (id,))
            rows = curs.fetchone()
            return rows
        except Exception as e:
            print("Database error:", e)
            return None
        finally:
            conn.close()

    rows = await get_cached_or_fetch(cache_key, fetch_data)

    if not rows:
        raise HTTPException(status_code=404, detail="User not found.")

    return {'result': rows}

@mypage_router.put('/{id}')
async def update_mypage(id: str, name: str = None):
    conn = hosts.connect()
    redis_client = await hosts.get_redis_connection()
    try:
        curs = conn.cursor()
        sql = "UPDATE user SET name=%s WHERE id=%s"
        curs.execute(sql, (name, id))
        conn.commit()

        cache_key = generate_cache_key("select_mypage", {"id": id})
        await redis_client.delete(cache_key)

        return {'result': "ok"}
    except Exception as e:
        print("Error:", e)
        return {'result': 'error'}
    finally:
        conn.close()

@mypage_router.put('/{id}/all')
async def update_all(id: str, name: str = None, image: str = None):
    conn = hosts.connect()
    redis_client = await hosts.get_redis_connection()
    try:
        curs = conn.cursor()
        sql = "UPDATE user SET name=%s, image=%s WHERE id=%s"
        curs.execute(sql, (name, image, id))
        conn.commit()

        cache_key = generate_cache_key("select_mypage", {"id": id})
        await redis_client.delete(cache_key)

        return {'result': "ok"}
    except Exception as e:
        print("Error:", e)
        return {'result': 'error'}
    finally:
        conn.close()

@mypage_router.get('/view/{file_name}')
async def get_user_image(file_name: str):
    try:
        file_obj = hosts.s3.get_object(Bucket=hosts.BUCKET_NAME, Key=file_name)
        file_data = file_obj['Body'].read()
        return StreamingResponse(io.BytesIO(file_data), media_type="image/jpeg")
    except ClientError as e:
        print(f"Error fetching file: {file_name}. Error: {e}")
        return {"result": "Error", "message": "File not found in S3."}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"result": "Error", "message": str(e)}

@mypage_router.post("/userimage")
async def upload_file(file: UploadFile = File(...)):
    try:
        s3_key = file.filename
        hosts.s3.upload_fileobj(file.file, hosts.BUCKET_NAME, s3_key)
        return {'result': 'OK', 's3_key': s3_key}
    except NoCredentialsError:
        return {'result': 'Error', 'message': 'AWS credentials not available.'}
    except Exception as e:
        print("Error:", e)
        return {'result': 'Error', 'message': str(e)}

@mypage_router.delete("/{file_name}")
async def delete_file(file_name: str):
    try:
        hosts.s3.delete_object(Bucket=hosts.BUCKET_NAME, Key=file_name)
        return {"result": "OK", "message": f"File {file_name} deleted successfully from bucket {hosts.BUCKET_NAME}"}
    except ClientError as e:
        print(f"Error deleting file: {file_name}. Error: {e}")
        return {"result": "Error", "message": str(e)}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"result": "Error", "message": str(e)}
