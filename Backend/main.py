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
    cred_path = "serviceAccountKey.json" # Local
else:
    cred_path = "/etc/secrets/serviceAccountKey.json" # Render

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
        print("✅ Original Uploaded")
        
        # 2. HLS Conversion (Optimized & Debug Enabled)
        hls_output_dir = os.path.join(temp_dir, "hls")
        os.makedirs(hls_output_dir, exist_ok=True)
        hls_local_path = os.path.join(hls_output_dir, "master.m3u8")

        print("⚡ Starting FFmpeg...")
        
        try:
            (
                ffmpeg.input(file_path)
                .output(hls_local_path, format='hls', start_number=0, hls_time=4, hls_list_size=0,
                        # FIX: scale=480:-2 ensures height is divisible by 2
                        vf='scale=480:-2', 
                        video_bitrate='800k', 
                        audio_bitrate='64k', 
                        acodec='aac', 
                        vcodec='libx264',
                        preset='ultrafast', 
                        threads=1,
                        pix_fmt='yuv420p')
                .run(capture_stdout=True, capture_stderr=True)
            )
            print("⚡ FFmpeg Done")

            # 3. Upload Segments
            hls_s3_folder = f"stream/{video_id}"
            for root, dirs, files in os.walk(hls_output_dir):
                for file in files:
                    local_file = os.path.join(root, file)
                    s3_key = f"{hls_s3_folder}/{file}"
                    ct = 'application/x-mpegURL' if file.endswith('.m3u8') else 'video/MP2T'
                    s3_client.upload_file(local_file, R2_BUCKET_NAME, s3_key, ExtraArgs={'ContentType': ct})
            
            print("✅ HLS Uploaded")

            # 4. Firestore Save
            # ✅ CHANGE: Ab downloadRef me FULL URL store hoga
            full_download_url = f"{R2_PUBLIC_DOMAIN}/{original_s3_key}"
            
            doc_data = {
                'title': title,
                'category': category,
                'description': text,
                'videoUrl': f"{R2_PUBLIC_DOMAIN}/{hls_s3_folder}/master.m3u8",
                'downloadRef': full_download_url, # <-- Ab Direct Link
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

        except ffmpeg.Error as e:
            error_log = e.stderr.decode('utf8')
            print(f"❌ FFmpeg CRASHED: {error_log}")

    except Exception as e:
        print(f"❌ General Processing Error: {e}")
    
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

# --- GET ALL VIDEOS ---
@app.get("/videos")
def get_videos():
    try:
        docs = db.collection('hooks').order_by('createdAt', direction=firestore.Query.DESCENDING).stream()
        videos = []
        for doc in docs:
            data = doc.to_dict()
            data['id'] = doc.id 
            videos.append(data)
        return {"videos": videos}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- DELETE VIDEO (UPDATED FOR FULL URL) ---
@app.delete("/delete-video/{video_id}")
def delete_video(video_id: str):
    try:
        doc_ref = db.collection('hooks').document(video_id)
        doc = doc_ref.get()

        if not doc.exists:
            raise HTTPException(status_code=404, detail="Video not found")

        data = doc.to_dict()
        
        # A. Delete Original File
        full_url = data.get('downloadRef')
        original_key = None
        
        # ✅ CHANGE: Full URL se "Key" extract karna zaroori hai delete ke liye
        if full_url and R2_PUBLIC_DOMAIN in full_url:
            original_key = full_url.replace(f"{R2_PUBLIC_DOMAIN}/", "")
        elif full_url:
            original_key = full_url # Purane videos ke liye jahan sirf key thi

        if original_key:
            try:
                s3_client.delete_object(Bucket=R2_BUCKET_NAME, Key=original_key)
                print(f"🗑️ Deleted Original R2 File: {original_key}")
            except Exception as e:
                print(f"⚠️ Error deleting original from R2: {e}")

        # B. Delete HLS Folder
        file_uuid = None
        if original_key:
            # "originals/UUID.mp4" se UUID nikalna
            file_uuid = original_key.split('/')[-1].replace('.mp4', '')
        
        if file_uuid:
            hls_prefix = f"stream/{file_uuid}/"
            try:
                objects = s3_client.list_objects_v2(Bucket=R2_BUCKET_NAME, Prefix=hls_prefix)
                if 'Contents' in objects:
                    delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                    s3_client.delete_objects(Bucket=R2_BUCKET_NAME, Delete={'Objects': delete_keys})
                    print(f"🗑️ Deleted HLS Folder: {hls_prefix}")
            except Exception as e:
                print(f"⚠️ Error deleting HLS from R2: {e}")

        # C. Delete from Firestore
        doc_ref.delete()
        print(f"🔥 Deleted from Firestore: {video_id}")

        return {"message": "Video & All Files Deleted Successfully"}

    except Exception as e:
        print(f"❌ Delete Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- UPDATE VIDEO ---
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
