# Python 3.9 Slim version use karenge
FROM python:3.9-slim

# Working directory set karenge
WORKDIR /app

# Note: FFmpeg hata diya gaya hai kyunki ab direct upload hai

# Saari files copy karenge
COPY . .

# Python libraries install karenge
RUN pip install --no-cache-dir -r requirements.txt

# Server start command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
