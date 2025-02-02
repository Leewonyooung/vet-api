"""
author: Aeong
Description: pet with Redis Caching
Fixed: 24.10.14
Usage: Manage Pet
"""

from fastapi import APIRouter, HTTPException, File, UploadFile, Form
import os
import shutil
import hosts, auth
from botocore.exceptions import NoCredentialsError
import json

router = APIRouter()

UPLOAD_DIRECTORY = "uploads/"  # 이미지 저장 경로

# 이미지 저장 디렉터리 확인 및 생성
if not os.path.exists(UPLOAD_DIRECTORY):
    os.makedirs(UPLOAD_DIRECTORY)

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

# 반려동물 조회
@router.get("/")
async def get_pets(user_id: str):
    cache_key = generate_cache_key("get_pets", {"user_id": user_id})

    async def fetch_data():
        conn = hosts.connect()
        try:
            with conn.cursor() as cursor:
                sql = "SELECT * FROM pet WHERE user_id = %s"
                cursor.execute(sql, (user_id,))
                pets = cursor.fetchall()
                return [
                    {
                        "id": pet[0],
                        "user_id": pet[1],
                        "species_type": pet[2],
                        "species_category": pet[3],
                        "name": pet[4],
                        "birthday": pet[5],
                        "features": pet[6],
                        "gender": pet[7],
                        "image": pet[8],
                    }
                    for pet in pets
                ]
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            conn.close()

    pets = await get_cached_or_fetch(cache_key, fetch_data)

    if not pets:
        raise HTTPException(status_code=404, detail="No pets found for this user.")

    return {"results": pets}

# 반려동물 등록
@router.post("/")
async def add_pet(
    id: str = Form(...),
    user_id: str = Form(...),
    species_type: str = Form(...),
    species_category: str = Form(...),
    name: str = Form(...),
    birthday: str = Form(...),
    features: str = Form(...),
    gender: str = Form(...),
    image: UploadFile = File(None)
):
    redis_client = await hosts.get_redis_connection()
    image_filename = ""
    if image:
        image_filename = image.filename
        image_path = os.path.join(UPLOAD_DIRECTORY, image_filename)

        with open(image_path, "wb") as buffer:
            shutil.copyfileobj(image.file, buffer)

    conn = hosts.connect()
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO pet (id, user_id, species_type, species_category, name, birthday, features, gender, image)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                id, user_id, species_type, species_category, name, 
                birthday, features, gender, image_filename
            ))
            conn.commit()

            cache_key = generate_cache_key("get_pets", {"user_id": user_id})
            await redis_client.delete(cache_key)

            return {"message": "Pet added successfully!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# 반려동물 수정
@router.put("/")
async def update_pet(
    id: str = Form(...),
    user_id: str = Form(...),
    species_type: str = Form(...),
    species_category: str = Form(...),
    name: str = Form(...),
    birthday: str = Form(...),
    features: str = Form(...),
    gender: str = Form(...),
    image: UploadFile = File(None)
):
    redis_client = await hosts.get_redis_connection()
    conn = hosts.connect()
    try:
        with conn.cursor() as cursor:
            if image:
                s3_key = f"pets/{user_id}/{image.filename}"
                try:
                    hosts.s3.upload_fileobj(image.file, hosts.BUCKET_NAME, s3_key)
                    image_url = f"https://{hosts.BUCKET_NAME}.s3.{hosts.REGION}.amazonaws.com/{s3_key}"
                except NoCredentialsError:
                    raise HTTPException(status_code=500, detail="AWS credentials not available.")
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Failed to upload image to S3: {str(e)}")

                sql = """
                    UPDATE pet 
                    SET species_type = %s, species_category = %s, name = %s, 
                        birthday = %s, features = %s, gender = %s, image = %s
                    WHERE id = %s AND user_id = %s
                """
                cursor.execute(sql, (
                    species_type, species_category, name, birthday,
                    features, gender, image_url, id, user_id
                ))
            else:
                sql = """
                    UPDATE pet 
                    SET species_type = %s, species_category = %s, name = %s, 
                        birthday = %s, features = %s, gender = %s
                    WHERE id = %s AND user_id = %s
                """
                cursor.execute(sql, (
                    species_type, species_category, name, birthday,
                    features, gender, id, user_id
                ))

            conn.commit()

            cache_key = generate_cache_key("get_pets", {"user_id": user_id})
            await redis_client.delete(cache_key)

            return {"message": "Pet updated successfully!"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# 반려동물 삭제
@router.delete("/{pet_id}")
async def delete_pet(pet_id: str, id: str):
    redis_client = await hosts.get_redis_connection()
    conn = hosts.connect()
    try:
        with conn.cursor() as cursor:
            sql = "DELETE FROM pet WHERE id = %s"
            result = cursor.execute(sql, (pet_id,))
            conn.commit()

            if result == 0:
                raise HTTPException(status_code=404, detail="Pet not found.")

            cache_key = generate_cache_key("get_pets", {"user_id": id})
            await redis_client.delete(cache_key)

            return {"message": "Pet deleted successfully!"}
    finally:
        conn.close()
