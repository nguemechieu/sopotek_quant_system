# ------------------------------------------------
# Base Image
# ------------------------------------------------
FROM python:3.11-slim

# ------------------------------------------------
# Environment Variables
# ------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ------------------------------------------------
# System Dependencies
# ------------------------------------------------
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------
# Working Directory
# ------------------------------------------------
WORKDIR /app

# ------------------------------------------------
# Copy Project Files
# ------------------------------------------------
COPY requirements.txt .

# ------------------------------------------------
# Install Python Dependencies
# ------------------------------------------------
RUN pip install --upgrade pip

RUN pip install -r requirements.txt

# ------------------------------------------------
# Copy Application Source
# ------------------------------------------------
COPY . .

# ------------------------------------------------
# Environment File
# ------------------------------------------------
COPY .env .env

# ------------------------------------------------
# Default Command
# ------------------------------------------------
CMD ["python", "-m", "sopotek_trading.main"]