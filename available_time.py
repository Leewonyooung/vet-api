"""
author: 
Description: Redis caching integration for available clinic and reservation management
Fixed: 
Usage: 
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
import os, json
import hosts

router = APIRouter()

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

    # Cache miss, fetch from DB
    data = await fetch_func()
    try:
        await redis_client.set(cache_key, json.dumps(data), ex=3600)
    except Exception as e:
        print(f"Redis set error: {e}")
    return data

# 예약 가능한 병원id, 이름, password, 경도, 위도, 주소, 이미지, 예약 시간 (예약된 리스트 빼고 나타냄)
@router.get('/available_clinic')
async def get_available_clinic(time: str):
    cache_key = generate_cache_key("available_clinic", {"time": time})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = """
            SELECT 
                c.id, c.name, c.latitude, c.longitude, c.address, c.image, ava.time
            FROM 
                clinic c LEFT OUTER JOIN
            (SELECT a.clinic_id, a.time 
             FROM available_time a LEFT OUTER JOIN reservation r 
             ON (a.time = r.time AND a.clinic_id = r.clinic_id) 
             WHERE r.time IS NULL AND a.time = %s) AS ava 
            ON (c.id = ava.clinic_id)
            WHERE ava.time IS NOT NULL
            """
            curs.execute(sql, (time,))
            rows = curs.fetchall()
            return rows
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    rows = await get_cached_or_fetch(cache_key, fetch_data)

    if not rows:
        raise HTTPException(status_code=404, detail="예약가능한 병원이 없습니다.")

    return {"results": rows}


@router.get('/available_clinic_noredis')
async def get_available_clinic_noredis(time: str):

    conn = hosts.connect()
    try:
        curs = conn.cursor()
        sql = """
        SELECT 
            c.id, c.name, c.latitude, c.longitude, c.address, c.image, ava.time
        FROM 
            clinic c LEFT OUTER JOIN
        (SELECT a.clinic_id, a.time 
            FROM available_time a LEFT OUTER JOIN reservation r 
            ON (a.time = r.time AND a.clinic_id = r.clinic_id) 
            WHERE r.time IS NULL AND a.time = %s) AS ava 
        ON (c.id = ava.clinic_id)
        WHERE ava.time IS NOT NULL
        """
        curs.execute(sql, (time,))
        rows = curs.fetchall()
        return rows
    except Exception as e:
        print("Database error:", e)
        return []
    finally:
        conn.close()



@router.get("/view/{file_name}")
async def get_file(file_name: str):
    file_path = os.path.join(UPLOAD_FOLDER, file_name)
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=file_name)
    return {"result": "Error"}

# clinic_info, location에서 예약 버튼 활성화 관리
@router.get("/can_reservation")
async def can_reservation(time: str = None, clinic_id: str = None):
    cache_key = generate_cache_key("can_reservation", {"time": time, "clinic_id": clinic_id})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = """
                SELECT 
                    c.name, c.latitude, c.longitude, c.address, c.image, ava.time, c.id
                FROM 
                    clinic c LEFT OUTER JOIN
                (SELECT a.clinic_id, a.time 
                 FROM available_time a LEFT OUTER JOIN reservation r 
                 ON (a.time = r.time AND a.clinic_id = r.clinic_id) 
                 WHERE r.time IS NULL AND a.time = %s) AS ava 
                ON (c.id = ava.clinic_id)
                WHERE ava.time IS NOT NULL AND c.id = %s
                """
            curs.execute(sql, (time, clinic_id))
            rows = curs.fetchone()
            return rows
        except Exception as e:
            print("Database error:", e)
            return None
        finally:
            conn.close()

    result = await get_cached_or_fetch(cache_key, fetch_data)
    return {"result": result}
