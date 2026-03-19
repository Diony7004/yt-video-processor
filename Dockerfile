FROM node:20-slim

# Instalar Python 3.11, ffmpeg y dependencias
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    node --version && python3 --version

WORKDIR /app

# Crear venv para evitar conflictos con system packages
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY main.py .

# Crear directorio de trabajo
RUN mkdir -p /tmp/yt-processor

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
