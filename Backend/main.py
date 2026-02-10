import os
import shutil
import uuid
import boto3
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException
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

# --- DIRECT UPLOAD TASK (No FFmpeg) ---
def direct_upload_task(file_path, original_filename, title, category, text, duration, language, is_premium, content_type):
    try:
        print(f"🎬 Uploading started for: {title}")
        
        # 1. Generate Unique Filename (Preserve Extension)
        file_ext = os.path.splitext(original_filename)[1] # e.g., .mp4, .mov
        if not file_ext:
            file_ext = ".mp4" # Default fallback
            
        unique_name = f"{uuid.uuid4()}{file_ext}"
        s3_key = f"videos/{unique_name}" # Folder structure: videos/xyz.mp4
        
        # 2. Upload to R2 (Direct)
        s3_client.upload_file(
            file_path, 
            R2_BUCKET_NAME, 
            s3_key, 
            ExtraArgs={'ContentType': content_type}
        )
        print("✅ Video Uploaded to R2")
        
        # 3. Construct URL
        full_url = f"{R2_PUBLIC_DOMAIN}/{s3_key}"
        
        # 4. Save to Firestore
        doc_data = {
            'title': title,
            'category': category,
            'description': text,
            'videoUrl': full_url,    # Same Direct URL (Stream)
            'downloadRef': full_url, # Same Direct URL (Download)
            'duration': duration,
            'language': language,
            'isPremium': is_premium,
            'createdAt': firestore.SERVER_TIMESTAMP,
            'processed': True,       # No processing needed, so True immediately
            'type': 'video',
            'views': 0, 'likes': 0
        }
        db.collection('hooks').add(doc_data)
        print("🔥 Firestore Updated Successfully")

    except Exception as e:
        print(f"❌ Upload Error: {e}")
    
    finally:
        # Cleanup Local Temp File
        if os.path.exists(file_path): 
            os.remove(file_path)

# --- API ENDPOINTS ---

@app.get("/")
def home():
    return {"status": "Backend is Running (Direct Upload Mode) 🚀"}

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
    # Save temp file locally first to ensure upload from App is complete
    temp_filename = f"temp_{uuid.uuid4()}_{file.filename}"
    with open(temp_filename, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Start Background Task
    background_tasks.add_task(
        direct_upload_task, 
        temp_filename, 
        file.filename, 
        title, 
        category, 
        text, 
        duration, 
        language, 
        is_premium,
        file.content_type
    )
    
    return {"message": "Upload started", "status": "uploading"}

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

# --- DELETE VIDEO ---
@app.delete("/delete-video/{video_id}")
def delete_video(video_id: str):
    try:
        doc_ref = db.collection('hooks').document(video_id)
        doc = doc_ref.get()

        if not doc.exists:
            raise HTTPException(status_code=404, detail="Video not found")

        data = doc.to_dict()
        full_url = data.get('videoUrl') # Using videoUrl as main ref
        
        # Delete from R2
        if full_url and R2_PUBLIC_DOMAIN in full_url:
            file_key = full_url.replace(f"{R2_PUBLIC_DOMAIN}/", "")
            try:
                s3_client.delete_object(Bucket=R2_BUCKET_NAME, Key=file_key)
                print(f"🗑️ Deleted R2 File: {file_key}")
            except Exception as e:
                print(f"⚠️ Error deleting from R2: {e}")

        # Delete from Firestore
        doc_ref.delete()
        print(f"🔥 Deleted from Firestore: {video_id}")

        return {"message": "Video Deleted Successfully"}

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
