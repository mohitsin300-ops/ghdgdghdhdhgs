# Python ka chhota version use karenge
FROM python:3.9-slim

# Working directory set karenge
WORKDIR /app

# System update karke FFmpeg install karenge (Video processing ke liye zaroori)
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get clean

# Saari files (main.py, json key, requirements) copy karenge
COPY . .

# Python libraries install karenge
RUN pip install --no-cache-dir -r requirements.txt

# Server start karenge (Render apne aap PORT set karega)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
