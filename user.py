"""
author: changbin an
Description: db for the user account with Redis Caching
Fixed: 07/Oct/2024
Usage: store user (including clinic) account information
"""

from fastapi import APIRouter, HTTPException
import hosts, json

router = APIRouter()

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

## Check User account from db  (안창빈)
@router.get("/selectuser")
async def select_user(id: str):
    cache_key = generate_cache_key("select_user", {"id": id})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = "SELECT id, password, image, name, phone FROM user WHERE id=%s"
            curs.execute(sql, (id,))
            rows = curs.fetchall()
            return [{'id': row[0], 'password': row[1], 'image': row[2], 'name': row[3], 'phone': row[4]} for row in rows]
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    result = await get_cached_or_fetch(cache_key, fetch_data)
    return {"results": result}

## Add Google account to sql db if it is a new user  (안창빈)
@router.get("/insertuser")
async def insert_user(id: str, password: str = None, image: str = None, name: str = None, phone: str = None):
    conn = hosts.connect()
    redis_client = await hosts.get_redis_connection()
    try:
        curs = conn.cursor()
        sql = "INSERT INTO user (id, password, image, name, phone) VALUES (%s, %s, %s, %s, %s)"
        curs.execute(sql, (id, password, image, name, phone))
        conn.commit()

        # Redis cache invalidation
        cache_key = generate_cache_key("select_user", {"id": id})
        await redis_client.delete(cache_key)

        return {"results": "OK"}
    except Exception as e:
        conn.rollback()
        print("Error:", e)
        return {"result": "Error"}
    finally:
        conn.close()

## Check clinic account from db  (안창빈)
@router.get("/selectclinic")
async def select_clinic(id: str, password: str = None):
    cache_key = generate_cache_key("select_clinic", {"id": id, "password": password})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = "SELECT id, password FROM clinic WHERE id=%s AND password=%s"
            curs.execute(sql, (id, password))
            rows = curs.fetchall()
            return [{'id': row[0], 'password': row[1]} for row in rows]
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    result = await get_cached_or_fetch(cache_key, fetch_data)
    return {"results": result}

"""
author: 이원영
Fixed: 2024/10/7
Usage: 채팅창 보여줄 때 id > name
"""
@router.get('/get_user_name')
async def get_user_name(id: str):
    cache_key = generate_cache_key("get_user_name", {"id": id})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = "SELECT name FROM user WHERE id = %s"
            curs.execute(sql, (id,))
            rows = curs.fetchall()
            return rows[0] if rows else None
        except Exception as e:
            print("Database error:", e)
            return None
        finally:
            conn.close()

    result = await get_cached_or_fetch(cache_key, fetch_data)

    if not result:
        raise HTTPException(status_code=404, detail="User name not found.")

    return {"results": result}
