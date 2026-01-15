import os
import shutil
import uuid
import boto3
import ffmpeg
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException
from botocore.config import Config
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURATION ---
R2_ACCOUNT_ID = "2d7e0facfb0d1a8789c41df977ceb223"
R2_ACCESS_KEY = "1c8bff9f80185c74e81fea005494c5f9"
R2_SECRET_KEY = "fc98ed9d6b2387f81e50d69794ef0be1402cc4474c7b3c427c896cb806220997"
R2_BUCKET_NAME = "shorts-videos"
R2_PUBLIC_DOMAIN = "https://pub-aae4a510a0ba4c71889c892e5010a7b1.r2.dev"

app = FastAPI()

# --- FIREBASE SETUP (SMART PATH) ---
# Ye check karega ki code Laptop par chal raha hai ya Render par
if os.path.exists("serviceAccountKey.json"):
    cred_path = "serviceAccountKey.json"  # Local File
    print("✅ Local Environment Detected: Using local key.")
else:
    cred_path = "/etc/secrets/serviceAccountKey.json" # Render Secret Path
    print("✅ Render Environment Detected: Using secret key.")

try:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("🔥 Firebase Connected Successfully")
except Exception as e:
    print(f"❌ Firebase Connection Failed: {e}")
    # App crash na ho isliye hum yahan pass kar rahe hain, par logs check karna zaroori hai
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

# --- CORE PROCESSING FUNCTION (Background Task) ---
def process_video_task(
    file_path: str, 
    original_filename: str, 
    title: str, 
    category: str, 
    text: str, 
    duration: int,
    language: str,
    is_premium: bool
):
    try:
        print(f"🎬 Processing started for: {title}")
        
        # 1. Unique ID Generation
        video_id = str(uuid.uuid4())
        
        # Paths Setup
        temp_dir = f"temp_{video_id}"
        os.makedirs(temp_dir, exist_ok=True)
        
        # --- OUTPUT 1: ORIGINAL UPLOAD (Private) ---
        original_s3_key = f"originals/{video_id}.mp4"
        
        print("📤 Uploading Original Video to R2...")
        s3_client.upload_file(
            file_path, 
            R2_BUCKET_NAME, 
            original_s3_key,
            ExtraArgs={'ContentType': 'video/mp4'}
        )
        print("✅ Original Uploaded")

        # --- OUTPUT 2: HLS CONVERSION (FFmpeg) ---
        hls_output_dir = os.path.join(temp_dir, "hls")
        os.makedirs(hls_output_dir, exist_ok=True)
        
        hls_filename = "master.m3u8"
        hls_local_path = os.path.join(hls_output_dir, hls_filename)

        print("⚡ Starting FFmpeg Conversion...")
        # FFmpeg Command
        (
            ffmpeg
            .input(file_path)
            .output(
                hls_local_path, 
                format='hls', 
                start_number=0, 
                hls_time=4, 
                hls_list_size=0,
                vf='scale=720:1280:force_original_aspect_ratio=decrease', 
                video_bitrate='1500k', 
                audio_bitrate='128k',
                acodec='aac', 
                vcodec='libx264',
                preset='fast'
            )
            .run(quiet=True, overwrite_output=True)
        )
        print("⚡ FFmpeg Conversion Done")

        # --- OUTPUT 3: UPLOAD HLS FILES ---
        hls_s3_folder = f"stream/{video_id}"
        print("📤 Uploading HLS Segments...")
        
        for root, dirs, files in os.walk(hls_output_dir):
            for file in files:
                local_file = os.path.join(root, file)
                s3_key = f"{hls_s3_folder}/{file}"
                
                content_type = 'application/x-mpegURL' if file.endswith('.m3u8') else 'video/MP2T'
                
                s3_client.upload_file(
                    local_file, 
                    R2_BUCKET_NAME, 
                    s3_key,
                    ExtraArgs={'ContentType': content_type}
                )

        print("✅ HLS Streaming Files Uploaded")

        # --- 4. FIREBASE UPDATE ---
        stream_url = f"{R2_PUBLIC_DOMAIN}/{hls_s3_folder}/master.m3u8"
        download_ref = original_s3_key

        doc_data = {
            'title': title,
            'category': category,
            'description': text,          # Flutter 'text' bhejta hai, hum 'description' mein save karte hain
            'videoUrl': stream_url,
            'downloadRef': download_ref,
            'thumbnailUrl': '',           
            'duration': duration,
            'language': language,
            'isPremium': is_premium,
            'isTrending': False,
            'likes': 0,
            'shares': 0,
            'views': 0,
            'createdAt': firestore.SERVER_TIMESTAMP,
            'processed': True,
            'type': 'video'
        }

        print(f"📝 Saving to Firestore: {doc_data['title']}")
        db.collection('hooks').add(doc_data)
        print("🔥 Firestore Updated Successfully")

    except Exception as e:
        print(f"❌ FATAL ERROR in Processing: {str(e)}")
        # Yahan hume pata chalega agar FFmpeg ya Firestore fail hua
    
    finally:
        # Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(file_path):
            os.remove(file_path)

# --- API ENDPOINTS ---

@app.get("/")
def home():
    return {"status": "Backend is Running 🚀"}

@app.post("/upload-video")
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    title: str = Form(...),
    category: str = Form(...),
    text: str = Form(...),
    duration: int = Form(...),
    language: str = Form("hinglish"),
    is_premium: bool = Form(False)
):
    print(f"📥 Received Upload Request: {title}")
    
    # Save upload locally
    temp_filename = f"temp_upload_{uuid.uuid4()}.mp4"
    with open(temp_filename, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Background Task
    background_tasks.add_task(
        process_video_task, 
        temp_filename, 
        file.filename, 
        title, 
        category, 
        text, 
        duration, 
        language, 
        is_premium
    )

    return {"message": "Upload accepted", "status": "processing"}

@app.post("/generate-download-link")
def generate_download_link(video_path: str):
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': R2_BUCKET_NAME, 'Key': video_path},
            ExpiresIn=300
        )
        return {"downloadUrl": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
