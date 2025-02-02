"""
author: 
Description: Reservation API with Redis Caching
Fixed: 
Usage: 
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

# 긴급예약에서 예약하기 눌렀을 시 예약DB에 저장
@router.post('/{user_id}')
async def insert_reservation(clinic_id: str, time: str, symptoms: str, pet_id: str, user_id: str):
    conn = hosts.connect()
    redis_client = await hosts.get_redis_connection()
    try:
        curs = conn.cursor()
        sql = "INSERT INTO reservation(user_id, clinic_id, time, symptoms, pet_id) VALUES (%s, %s, %s, %s, %s)"
        curs.execute(sql, (user_id, clinic_id, time, symptoms, pet_id))
        conn.commit()

        # Redis 캐시 무효화
        cache_key = generate_cache_key("select_reservation", {"user_id": user_id})
        await redis_client.delete(cache_key)

        return {'results': 'OK'}
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to insert reservation.")
    finally:
        conn.close()

# 예약내역 보여주는 리스트
@router.get('/user/{user_id}')
async def select_reservation(user_id: str):
    cache_key = generate_cache_key("select_reservation", {"user_id": user_id})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = '''
            SELECT clinic.id, clinic.name, clinic.latitude, clinic.longitude, reservation.time, clinic.address 
            FROM reservation, clinic 
            WHERE reservation.clinic_id = clinic.id AND user_id = %s
            '''
            curs.execute(sql, (user_id,))
            rows = curs.fetchall()
            return rows
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    rows = await get_cached_or_fetch(cache_key, fetch_data)
    return {'results': rows}

# 병원에서 보는 예약 현황
@router.get('/clinic/{clinic_id}')
async def select_reservation_clinic(clinic_id: str, time: str):
    cache_key = generate_cache_key("select_reservation_clinic", {"clinic_id": clinic_id, "time": time})

    async def fetch_data():
        conn = hosts.connect()
        try:
            curs = conn.cursor()
            sql = '''
            SELECT user.name, res.species_type, res.species_category, res.features, res.symptoms, res.time
            FROM user,
                (SELECT reservation.user_id, pet.species_type, pet.species_category, pet.features, reservation.symptoms, reservation.time
                 FROM reservation 
                 INNER JOIN pet ON reservation.pet_id = pet.id AND clinic_id = %s) AS res
            WHERE res.user_id = user.id AND time LIKE %s ORDER BY time ASC
            '''
            time1 = f'{time}%'
            curs.execute(sql, (clinic_id, time1))
            rows = curs.fetchall()
            return rows
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    rows = await get_cached_or_fetch(cache_key, fetch_data)
    return {'results': rows}
