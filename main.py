"""
yt-video-processor - Microservicio para descargar videos de YouTube
y segmentarlos en chunks de 120 segundos para Gemini Embedding 2.

Endpoints:
  POST /process   - Descarga y segmenta un video de YouTube
  GET  /segments  - Lista segmentos disponibles de un video
  GET  /download  - Descarga un segmento específico
  GET  /health    - Health check
  DELETE /cleanup - Limpia archivos de un video procesado
"""

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
import subprocess
import os
import json
import uuid
import glob
import shutil

app = FastAPI(
    title="YT Video Processor",
    description="Descarga videos de YouTube y los segmenta en chunks de 120s para Gemini Embedding 2",
    version="1.0.0"
)

API_KEY = os.environ.get("API_KEY", "test-key-123")
PORT = int(os.environ.get("PORT", "8000"))
WORK_DIR = os.environ.get("WORK_DIR", "/tmp/yt-processor")
SEGMENT_DURATION = int(os.environ.get("SEGMENT_DURATION", "120"))

os.makedirs(WORK_DIR, exist_ok=True)


def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


class ProcessRequest(BaseModel):
    url: str
    segment_duration: int = SEGMENT_DURATION


class ProcessResponse(BaseModel):
    job_id: str
    title: str
    duration_seconds: float
    total_segments: int
    segments: list


@app.get("/health")
def health():
    yt_dlp = subprocess.run(["yt-dlp", "--version"], capture_output=True, text=True)
    ffmpeg = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True)
    return {
        "status": "ok",
        "service": "yt-video-processor",
        "timestamp": datetime.utcnow().isoformat(),
        "tools": {
            "yt-dlp": yt_dlp.stdout.strip() if yt_dlp.returncode == 0 else "NOT FOUND",
            "ffmpeg": "ok" if ffmpeg.returncode == 0 else "NOT FOUND"
        }
    }


@app.post("/process", dependencies=[__import__('fastapi').Depends(verify_api_key)])
def process_video(req: ProcessRequest):
    """Descarga un video de YouTube y lo segmenta en chunks."""

    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(WORK_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    # 1. Obtener metadata del video
    try:
        meta_result = subprocess.run(
            ["yt-dlp", "--dump-json", "--no-download", req.url],
            capture_output=True, text=True, timeout=60
        )
        if meta_result.returncode != 0:
            raise Exception(meta_result.stderr)
        meta = json.loads(meta_result.stdout)
    except Exception as e:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=f"Error obteniendo metadata: {str(e)}")

    title = meta.get("title", "unknown")
    duration = meta.get("duration", 0)

    # 2. Descargar video (mejor calidad que no exceda 720p para ahorrar espacio)
    video_path = os.path.join(job_dir, "full_video.mp4")
    try:
        dl_result = subprocess.run(
            [
                "yt-dlp",
                "-f", "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best",
                "--merge-output-format", "mp4",
                "-o", video_path,
                req.url
            ],
            capture_output=True, text=True, timeout=600
        )
        if dl_result.returncode != 0:
            raise Exception(dl_result.stderr)
    except subprocess.TimeoutExpired:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise HTTPException(status_code=408, detail="Timeout descargando video (max 10 min)")
    except Exception as e:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=f"Error descargando video: {str(e)}")

    # 3. Segmentar con ffmpeg
    segment_pattern = os.path.join(job_dir, "segment_%03d.mp4")
    try:
        seg_result = subprocess.run(
            [
                "ffmpeg", "-i", video_path,
                "-c", "copy",
                "-map", "0",
                "-segment_time", str(req.segment_duration),
                "-f", "segment",
                "-reset_timestamps", "1",
                segment_pattern
            ],
            capture_output=True, text=True, timeout=300
        )
        if seg_result.returncode != 0:
            raise Exception(seg_result.stderr)
    except Exception as e:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Error segmentando video: {str(e)}")

    # 4. Eliminar video completo para ahorrar espacio
    if os.path.exists(video_path):
        os.remove(video_path)

    # 5. Listar segmentos generados
    segment_files = sorted(glob.glob(os.path.join(job_dir, "segment_*.mp4")))
    segments = []
    for i, seg_path in enumerate(segment_files):
        start_sec = i * req.segment_duration
        end_sec = min(start_sec + req.segment_duration, duration)
        file_size = os.path.getsize(seg_path)
        segments.append({
            "index": i,
            "filename": os.path.basename(seg_path),
            "start_time": f"{int(start_sec // 60):02d}:{int(start_sec % 60):02d}",
            "end_time": f"{int(end_sec // 60):02d}:{int(end_sec % 60):02d}",
            "start_seconds": start_sec,
            "end_seconds": end_sec,
            "size_bytes": file_size,
            "download_url": f"/download?job_id={job_id}&segment={i}"
        })

    # Guardar metadata del job
    job_meta = {
        "job_id": job_id,
        "url": req.url,
        "title": title,
        "duration_seconds": duration,
        "segment_duration": req.segment_duration,
        "total_segments": len(segments),
        "segments": segments,
        "created_at": datetime.utcnow().isoformat()
    }
    with open(os.path.join(job_dir, "metadata.json"), "w") as f:
        json.dump(job_meta, f, indent=2)

    return job_meta


@app.get("/segments", dependencies=[__import__('fastapi').Depends(verify_api_key)])
def list_segments(job_id: str):
    """Lista los segmentos de un video procesado."""
    meta_path = os.path.join(WORK_DIR, job_id, "metadata.json")
    if not os.path.exists(meta_path):
        raise HTTPException(status_code=404, detail=f"Job {job_id} no encontrado")
    with open(meta_path) as f:
        return json.load(f)


@app.get("/download", dependencies=[__import__('fastapi').Depends(verify_api_key)])
def download_segment(job_id: str, segment: int):
    """Descarga un segmento específico como archivo MP4."""
    seg_path = os.path.join(WORK_DIR, job_id, f"segment_{segment:03d}.mp4")
    if not os.path.exists(seg_path):
        raise HTTPException(status_code=404, detail=f"Segmento {segment} no encontrado en job {job_id}")
    return FileResponse(
        seg_path,
        media_type="video/mp4",
        filename=f"{job_id}_segment_{segment:03d}.mp4"
    )


@app.get("/segment-base64", dependencies=[__import__('fastapi').Depends(verify_api_key)])
def segment_base64(job_id: str, segment: int):
    """Devuelve un segmento en base64 listo para enviar a Gemini Embedding API."""
    import base64
    seg_path = os.path.join(WORK_DIR, job_id, f"segment_{segment:03d}.mp4")
    if not os.path.exists(seg_path):
        raise HTTPException(status_code=404, detail=f"Segmento {segment} no encontrado en job {job_id}")

    file_size = os.path.getsize(seg_path)
    if file_size > 20 * 1024 * 1024:  # 20MB limit
        raise HTTPException(status_code=413, detail="Segmento demasiado grande para base64 (max 20MB)")

    with open(seg_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")

    return {
        "job_id": job_id,
        "segment": segment,
        "mime_type": "video/mp4",
        "size_bytes": file_size,
        "base64": b64
    }


@app.delete("/cleanup", dependencies=[__import__('fastapi').Depends(verify_api_key)])
def cleanup(job_id: str):
    """Elimina todos los archivos de un job procesado."""
    job_dir = os.path.join(WORK_DIR, job_id)
    if not os.path.exists(job_dir):
        raise HTTPException(status_code=404, detail=f"Job {job_id} no encontrado")
    shutil.rmtree(job_dir)
    return {"status": "cleaned", "job_id": job_id}


@app.get("/jobs", dependencies=[__import__('fastapi').Depends(verify_api_key)])
def list_jobs():
    """Lista todos los jobs disponibles."""
    jobs = []
    for d in os.listdir(WORK_DIR):
        meta_path = os.path.join(WORK_DIR, d, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            jobs.append({
                "job_id": meta["job_id"],
                "title": meta["title"],
                "total_segments": meta["total_segments"],
                "created_at": meta.get("created_at")
            })
    return {"jobs": jobs}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
