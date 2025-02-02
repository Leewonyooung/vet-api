"""
author: Aeong
Description: species with Redis Caching
Fixed: 2024.10.12
Usage: Manage species types and categories
"""

from fastapi import APIRouter, HTTPException, Query
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

# 모든 종류 조회 API (GET)
@router.get("/types")
async def get_species_types():
    conn = hosts.connect()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT DISTINCT type FROM species"
            cursor.execute(sql)
            types = cursor.fetchall()
            return [type[0] for type in types] if types else []
    except Exception as e:
        print("Database error:", e)
        return []
    finally:
        conn.close()


# 특정 종류의 세부 종류 조회 API (GET)
@router.get("/categories")
async def get_species_categories():
    conn = hosts.connect()
    try:
        curs = conn.cursor()
        sql = "SELECT category FROM species"
        curs.execute(sql)
        rows = curs.fetchall()
        return [row[0] for row in rows] if rows else []
    except Exception as e:
        print("Database error:", e)
        return []
    finally:
        conn.close()

# 특정 종류에 따른 세부 종류 조회 API
@router.get("/pet_categories")
async def get_species_categories(type: str):
    conn =hosts.connect()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT category FROM species WHERE type = %s"
            cursor.execute(sql, (type,))
            categories = cursor.fetchall()

            if not categories:
                raise HTTPException(status_code=404, detail="No categories found for this species type.")

            return [category[0] for category in categories]
    finally:
        conn.close()

# 새로운 종류 추가 API
@router.post("/")
async def add_species(species_category: str, id: str):
    conn = hosts.connect()
    redis_client = await hosts.get_redis_connection()
    try:
        curs = conn.cursor()
        sql = "INSERT INTO species (type, category) VALUES (%s, %s)"
        curs.execute(sql, ('강아지', species_category))
        conn.commit()

        # Redis 캐시 무효화
        cache_key = generate_cache_key("get_species_categories", {"user_id": id})
        await redis_client.delete(cache_key)

        return {"results": "OK"}
    except Exception as e:
        print("Error:", e)
        return {"result": "Error"}
    finally:
        conn.close()

# 종류 삭제 API (DELETE)
@router.delete("/")
async def delete_species(species_type: str, species_category: str, id: str):
    conn = hosts.connect()
    redis_client = await hosts.get_redis_connection()
    try:
        with conn.cursor() as cursor:
            sql = "DELETE FROM species WHERE type = %s AND category = %s"
            result = cursor.execute(sql, (species_type, species_category))
            conn.commit()

            if result == 0:
                raise HTTPException(status_code=404, detail="Species not found.")

            # Redis 캐시 무효화
            cache_key = generate_cache_key("get_species_categories", {"user_id": id})
            await redis_client.delete(cache_key)

            return {"message": "Species deleted successfully!"}
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Failed to delete species.")
    finally:
        conn.close()
