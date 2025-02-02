"""
author: 이원영
Description: 병원 테이블 API 핸들러
Fixed: 2024/10/7
Usage: 
"""

from fastapi import APIRouter, File, UploadFile, HTTPException
import os, json
import hosts,auth
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from fastapi.responses import StreamingResponse
import io
from auth import get_current_user

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

# [DELETE] 이미지 삭제
@router.delete("/images/{id}")
async def delete_image(id: str):
    conn = hosts.connect()
    curs = conn.cursor()
    try:
        sql = "DELETE FROM image WHERE id=%s"
        curs.execute(sql, (id,))
        conn.commit()
        return {"result": "OK"}
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Error deleting image")
    finally:
        conn.close()


# [POST] 파일 업로드 (S3)
@router.post("/files")
async def upload_file(file: UploadFile = File(...)):
    try:
        s3_key = file.filename
        hosts.s3.upload_fileobj(file.file, hosts.BUCKET_NAME, s3_key)
        return {'result': 'OK', 's3_key': s3_key}
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail='AWS credentials not available.')
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail=str(e))


# [GET] S3에서 파일 조회 (이미지 반환)
@router.get("/files/{file_name}")
async def get_file(file_name: str):
    cache_key = generate_cache_key("view_file", {"file_name": file_name})

    async def fetch_file():
        file_obj = hosts.s3.get_object(Bucket=hosts.BUCKET_NAME, Key=file_name)
        file_data = file_obj['Body'].read()
        return file_data

    file_data = await get_cached_or_fetch(cache_key, fetch_file)
    if not file_data:
        raise HTTPException(status_code=404, detail="File not found in S3.")
    return StreamingResponse(io.BytesIO(file_data), media_type="image/jpeg")


# ====================================
# Clinic(병원/클리닉) 관련 엔드포인트
# ====================================

# [GET] 특정 클리닉의 이름 조회 (ID로 조회)
@router.get("/{id}/name")
async def get_clinic_name_by_id(id: str):
    conn = hosts.connect()
    try:
        with conn.cursor() as curs:
            sql = "SELECT name FROM clinic WHERE id = %s"
            curs.execute(sql, (id,))
            row = curs.fetchone()
        return row
    except Exception as e:
        print("Database error:", e)
        return None
    finally:
        conn.close()



# [GET] 특정 클리닉의 ID 조회 (이름으로 조회)
@router.get("/by-name/{name}/id")
async def get_clinic_id_by_name(name: str):

    conn = hosts.connect()
    try:
        with conn.cursor() as curs:
            sql = "SELECT id FROM clinic WHERE name = %s"
            curs.execute(sql, (name,))
            row = curs.fetchone()
        return row
    except Exception as e:
        print("Database error:", e)
        return None
    finally:
        conn.close()


# [GET] 클리닉 목록 조회 (검색어 제공 시 이름 또는 주소로 검색)
@router.get("/")
async def list_clinics(search: str = None):
    # search 파라미터가 있으면 검색, 없으면 전체 목록 반환
    if search:
        cache_key = generate_cache_key("clinic_search", {"search": search})
    else:
        cache_key = generate_cache_key("clinic_list", {})

    async def fetch_data():
        conn = hosts.connect()
        try:
            with conn.cursor() as curs:
                if search:
                    sql = "SELECT * FROM clinic WHERE name LIKE %s OR address LIKE %s"
                    keyword = f"%{search}%"
                    curs.execute(sql, (keyword, keyword))
                else:
                    sql = "SELECT * FROM clinic"
                    curs.execute(sql)
                rows = curs.fetchall()
            return rows
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    results = await get_cached_or_fetch(cache_key, fetch_data)
    return {"results": results}


# [GET] 클리닉 상세정보 조회
@router.get("/{id}")
async def get_clinic_detail(id: str):

    conn = hosts.connect()
    try:
        with conn.cursor() as curs:
            sql = "SELECT * FROM clinic WHERE id=%s"
            curs.execute(sql, (id,))
            row = curs.fetchone()
        return row
    except Exception as e:
        print("Database error:", e)
        return None
    finally:
        conn.close()


# [POST] 새로운 클리닉 생성  
# (요청 body에는 JSON 형식으로 클리닉 데이터를 포함하도록 합니다.)
@router.post("/")
async def create_clinic(clinic: dict):  # 실제 프로젝트에서는 Pydantic 모델을 사용하는 것이 좋습니다.
    conn = hosts.connect()
    try:
        with conn.cursor() as curs:
            sql = """
            INSERT INTO clinic
            (id, name, password, latitude, longitude, start_time, end_time, introduction, address, phone, image)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            curs.execute(sql, (
                clinic.get("id"),
                clinic.get("name"),
                clinic.get("password"),
                clinic.get("latitude"),
                clinic.get("longitude"),
                clinic.get("starttime"),
                clinic.get("endtime"),
                clinic.get("introduction"),
                clinic.get("address"),
                clinic.get("phone"),
                clinic.get("image"),
            ))
            conn.commit()
        return {"result": "OK"}
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Error creating clinic")
    finally:
        conn.close()


# [PUT] 클리닉 정보 업데이트 (전체 정보 갱신)
@router.put("/{id}")
async def update_clinic(id: str, clinic: dict):  # 실제 프로젝트에서는 Pydantic 모델을 사용하는 것이 좋습니다.
    conn = hosts.connect()
    try:
        with conn.cursor() as curs:
            sql = """
            UPDATE clinic
            SET name = %s,
                password = %s,
                latitude = %s,
                longitude = %s,
                start_time = %s,
                end_time = %s,
                introduction = %s,
                address = %s,
                phone = %s,
                image = %s
            WHERE id = %s
            """
            curs.execute(sql, (
                clinic.get("name"),
                clinic.get("password"),
                clinic.get("latitude"),
                clinic.get("longitude"),
                clinic.get("starttime"),
                clinic.get("endtime"),
                clinic.get("introduction"),
                clinic.get("address"),
                clinic.get("phone"),
                clinic.get("image"),
                id
            ))
            conn.commit()
        return {"result": "OK"}
    except Exception as e:
        print("Error:", e)
        raise HTTPException(status_code=500, detail="Error updating clinic")
    finally:
        conn.close()


# [GET] 클리닉 카드용 간략 정보 조회 (예: 이름, 주소, 이미지)
@router.get("/cards")
async def get_clinic_cards():
    conn = hosts.connect()
    try:
        with conn.cursor() as curs:
            sql = "SELECT name, address, image FROM clinic"
            curs.execute(sql)
            rows = curs.fetchall()
        return {"results": rows}
    except Exception as e:
        print("Database error:", e)
        raise HTTPException(status_code=500, detail="Error fetching clinic cards")
    finally:
        conn.close()



@router.put("/{id}/all")
async def update_all(
    id: str ,
    name: str = None, 
    password: str = None, 
    latitude: str = None, 
    longitude: str = None, 
    starttime: str = None, 
    endtime: str = None, 
    introduction: str = None, 
    address: str = None, 
    phone: str = None, 
    image: str = None,
):
    conn = hosts.connect()
    try:
        with conn.cursor() as curs:
            sql = """
            UPDATE clinic
            SET name = %s,
            password = %s,
            latitude = %s,
            longitude = %s,
            start_time = %s,
            end_time = %s,
            introduction = %s,
            address = %s,
            phone = %s,
            image = %s
            WHERE id = %s
            """
            curs.execute(sql, (name, password, latitude, longitude, starttime, endtime, introduction, address, phone, image, id))
            conn.commit()
        return {"result": "OK"}
    except Exception as e:
        print("Error:", e)
        return {"result": "Error"}
    finally:
        conn.close()

