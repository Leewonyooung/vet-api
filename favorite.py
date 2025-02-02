"""
author: Aeong
Description: favorite (Redis Caching Applied)
Fixed: 24.10.14
Usage: Manage favorite
"""

from fastapi import APIRouter, HTTPException
import hosts
import json

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

    # Cache miss, fetch from DB
    data = await fetch_func()
    try:
        await redis_client.set(cache_key, json.dumps(data), ex=3600)
    except Exception as e:
        print(f"Redis set error: {e}")
    return data

# 사용자의 즐겨찾기 목록 불러오기
@router.get('/{user_id}')
async def get_favorite_clinics(user_id: str):
    cache_key = generate_cache_key("favorite_clinics", {"user_id": user_id})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = "SELECT * FROM favorite WHERE user_id = %s"
            curs.execute(sql, (user_id,))
            rows = curs.fetchall()
            return rows
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    rows = await get_cached_or_fetch(cache_key, fetch_data)

    if not rows:
        raise HTTPException(status_code=404, detail="즐겨찾기 병원이 없습니다.")
    
    return {'results': rows}

# 즐겨찾기 추가
@router.post('/')
async def add_favorite(clinic_id: str, user_id: str):
    conn = hosts.connect()
    try:
        curs = conn.cursor()

        # 중복 확인
        sql_check = "SELECT * FROM favorite WHERE user_id = %s AND clinic_id = %s"
        curs.execute(sql_check, (user_id, clinic_id))
        result = curs.fetchone()

        if result:
            raise HTTPException(status_code=400, detail="이미 즐겨찾기 목록에 있습니다.")

        # 즐겨찾기 추가 (clinic 테이블에서 데이터를 가져와 favorite 테이블에 삽입)
        sql = """
            INSERT INTO favorite (user_id, clinic_id, name, password, latitude, longitude, start_time, end_time, introduction, address, phone, image)
            SELECT %s, id, name, password, latitude, longitude, start_time, end_time, introduction, address, phone, image
            FROM clinic WHERE id = %s
        """
        curs.execute(sql, (user_id, clinic_id))
        conn.commit()


        return {"message": "즐겨찾기 병원이 추가되었습니다."}
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="즐겨찾기 추가 중 문제가 발생했습니다.")
    finally:
        conn.close()

# 즐겨찾기 삭제
@router.delete('/')
async def delete_favorite(clinic_id: str, user_id: str):
    conn = hosts.connect()
    try:
        curs = conn.cursor()

        # 즐겨찾기 삭제
        sql = "DELETE FROM favorite WHERE user_id = %s AND clinic_id = %s"
        result = curs.execute(sql, (user_id, clinic_id))
        conn.commit()

        if result == 0:
            raise HTTPException(status_code=404, detail="해당 병원이 즐겨찾기에 없습니다.")

        return {"message": "즐겨찾기 병원이 삭제되었습니다."}
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="즐겨찾기 삭제 중 문제가 발생했습니다.")
    finally:
        conn.close()

# 즐겨찾기 여부 검사
@router.get('/{user_id}/like')
async def search_favorite_clinic(clinic_id: str, user_id: str):
    conn = hosts.connect()
    try:
        curs = conn.cursor()
        sql = "SELECT COUNT(*) FROM favorite WHERE user_id = %s AND clinic_id = %s"
        curs.execute(sql, (user_id, clinic_id))
        rows = curs.fetchall()
        return rows[0][0]
    except Exception as e:
        print("Database error:", e)
        return 0
    finally:
        conn.close()