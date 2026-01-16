import os
import shutil
import uuid
import boto3
import ffmpeg
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Body
from botocore.config import Config
import firebase_admin
from firebase_admin import credentials, firestore
from pydantic import BaseModel

# --- CONFIGURATION ---
R2_ACCOUNT_ID = "2d7e0facfb0d1a8789c41df977ceb223"
R2_ACCESS_KEY = "1c8bff9f80185c74e81fea005494c5f9"
R2_SECRET_KEY = "fc98ed9d6b2387f81e50d69794ef0be1402cc4474c7b3c427c896cb806220997"
R2_BUCKET_NAME = "shorts-videos"
R2_PUBLIC_DOMAIN = "https://pub-aae4a510a0ba4c71889c892e5010a7b1.r2.dev"

app = FastAPI()

# --- FIREBASE SETUP ---
if os.path.exists("serviceAccountKey.json"):
    cred_path = "serviceAccountKey.json"
else:
    cred_path = "/etc/secrets/serviceAccountKey.json"

try:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("🔥 Firebase Connected Successfully")
except Exception as e:
    print(f"❌ Firebase Connection Failed: {e}")
    pass

# --- R2 CLIENT SETUP ---
s3_client = boto3.client(
    's3',
    endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='auto' 
)

# --- MODELS ---
class UpdateVideoModel(BaseModel):
    title: str
    description: str
    isPremium: bool

# --- CORE PROCESSING FUNCTION ---
def process_video_task(file_path, original_filename, title, category, text, duration, language, is_premium):
    try:
        print(f"🎬 Processing started for: {title}")
        video_id = str(uuid.uuid4())
        
        # Paths
        temp_dir = f"temp_{video_id}"
        os.makedirs(temp_dir, exist_ok=True)
        
        # 1. Original Upload
        original_s3_key = f"originals/{video_id}.mp4"
        s3_client.upload_file(file_path, R2_BUCKET_NAME, original_s3_key, ExtraArgs={'ContentType': 'video/mp4'})
        
        # 2. HLS Conversion (Optimized for Render Free Tier)
        hls_output_dir = os.path.join(temp_dir, "hls")
        os.makedirs(hls_output_dir, exist_ok=True)
        hls_local_path = os.path.join(hls_output_dir, "master.m3u8")

        (
            ffmpeg.input(file_path)
            .output(hls_local_path, format='hls', start_number=0, hls_time=4, hls_list_size=0,
                    vf='scale=480:854:force_original_aspect_ratio=decrease', 
                    video_bitrate='1000k', audio_bitrate='128k', acodec='aac', vcodec='libx264',
                    preset='ultrafast', threads=1)
            .run(quiet=True, overwrite_output=True)
        )

        # 3. Upload Segments
        hls_s3_folder = f"stream/{video_id}"
        for root, dirs, files in os.walk(hls_output_dir):
            for file in files:
                local_file = os.path.join(root, file)
                s3_key = f"{hls_s3_folder}/{file}"
                ct = 'application/x-mpegURL' if file.endswith('.m3u8') else 'video/MP2T'
                s3_client.upload_file(local_file, R2_BUCKET_NAME, s3_key, ExtraArgs={'ContentType': ct})

        # 4. Firestore Save
        doc_data = {
            'title': title,
            'category': category,
            'description': text,
            'videoUrl': f"{R2_PUBLIC_DOMAIN}/{hls_s3_folder}/master.m3u8",
            'downloadRef': original_s3_key,
            'duration': duration,
            'language': language,
            'isPremium': is_premium,
            'createdAt': firestore.SERVER_TIMESTAMP,
            'processed': True,
            'type': 'video',
            'views': 0, 'likes': 0
        }
        db.collection('hooks').add(doc_data)
        print("🔥 Firestore Updated Successfully")

    except Exception as e:
        print(f"❌ Processing Error: {e}")
    finally:
        if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
        if os.path.exists(file_path): os.remove(file_path)

# --- API ENDPOINTS ---

@app.get("/")
def home():
    return {"status": "Backend is Running 🚀"}

@app.post("/upload-video")
async def upload_video(background_tasks: BackgroundTasks, file: UploadFile = File(...), title: str = Form(...), category: str = Form(...), text: str = Form(...), duration: int = Form(...), language: str = Form("hinglish"), is_premium: bool = Form(False)):
    temp_filename = f"temp_upload_{uuid.uuid4()}.mp4"
    with open(temp_filename, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    background_tasks.add_task(process_video_task, temp_filename, file.filename, title, category, text, duration, language, is_premium)
    return {"message": "Upload accepted", "status": "processing"}

# --- NEW: GET ALL VIDEOS (For Admin List) ---
@app.get("/videos")
def get_videos():
    try:
        # Get all videos ordered by date
        docs = db.collection('hooks').order_by('createdAt', direction=firestore.Query.DESCENDING).stream()
        videos = []
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id # Document ID bhi chahiye update/delete ke liye
            videos.append(data)
        return {"videos": videos}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- NEW: DELETE VIDEO ---
@app.delete("/delete-video/{video_id}")
def delete_video(video_id: str):
    try:
        db.collection('hooks').document(video_id).delete()
        return {"message": "Video Deleted Successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- NEW: UPDATE VIDEO (Title, Description, Premium) ---
@app.put("/update-video/{video_id}")
def update_video(video_id: str, video: UpdateVideoModel):
    try:
        db.collection('hooks').document(video_id).update({
            'title': video.title,
            'description': video.description,
            'isPremium': video.isPremium
        })
        return {"message": "Video Updated Successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
