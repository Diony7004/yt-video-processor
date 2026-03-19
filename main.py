"""
yt-video-processor - Microservicio para descargar videos de YouTube,
segmentarlos, generar embeddings con Gemini y almacenar en Qdrant.

Endpoints:
  POST /ingest         - Pipeline completo: descarga → segmenta → embed → Qdrant (async)
  GET  /ingest-status  - Estado de un job de ingesta
  POST /process        - Solo descarga y segmenta (sync)
  GET  /segments       - Lista segmentos de un video
  GET  /download       - Descarga un segmento MP4
  GET  /segment-base64 - Segmento en base64
  GET  /jobs           - Lista todos los jobs
  DELETE /cleanup      - Limpia archivos de un job
  GET  /health         - Health check
"""

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Depends
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
import subprocess
import os
import json
import uuid
import glob
import shutil
import base64
import requests as http_requests

# Base args para yt-dlp: JS runtime + anti-bot
YT_DLP_BASE = ["yt-dlp", "--js-runtimes", "node", "--extractor-args", "youtube:player_client=mediaconnect"]

app = FastAPI(
    title="YT Video Processor",
    description="Descarga videos de YouTube, genera embeddings con Gemini 2 y almacena en Qdrant",
    version="2.0.0"
)

# === Config ===
API_KEY = os.environ.get("API_KEY", "test-key-123")
PORT = int(os.environ.get("PORT", "8000"))
WORK_DIR = os.environ.get("WORK_DIR", "/tmp/yt-processor")
SEGMENT_DURATION = int(os.environ.get("SEGMENT_DURATION", "120"))

# Gemini & Qdrant config
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
QDRANT_URL = os.environ.get("QDRANT_URL", "")
QDRANT_API_KEY = os.environ.get("QDRANT_API_KEY", "")
QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", "tech_provider")
EMBEDDING_DIMENSIONS = int(os.environ.get("EMBEDDING_DIMENSIONS", "768"))

os.makedirs(WORK_DIR, exist_ok=True)

# In-memory job status tracking
ingest_jobs = {}


def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


# === Models ===

class ProcessRequest(BaseModel):
    url: str
    segment_duration: int = SEGMENT_DURATION


class IngestRequest(BaseModel):
    url: str
    segment_duration: int = SEGMENT_DURATION
    video_title: str = ""


# === INGEST PIPELINE (new) ===

@app.post("/ingest", dependencies=[Depends(verify_api_key)])
def ingest_video(req: IngestRequest, background_tasks: BackgroundTasks):
    """Pipeline completo: descarga → segmenta → embed con Gemini → almacena en Qdrant.
    Ejecuta en background y retorna job_id para monitorear progreso."""

    if not GOOGLE_API_KEY:
        raise HTTPException(status_code=500, detail="GOOGLE_API_KEY no configurada")
    if not QDRANT_URL:
        raise HTTPException(status_code=500, detail="QDRANT_URL no configurada")
    if not QDRANT_API_KEY:
        raise HTTPException(status_code=500, detail="QDRANT_API_KEY no configurada")

    job_id = uuid.uuid4().hex[:12]
    ingest_jobs[job_id] = {
        "job_id": job_id,
        "status": "processing",
        "step": "queued",
        "progress": "0/0",
        "url": req.url,
        "title": "",
        "total_segments": 0,
        "embedded_segments": 0,
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "error": None
    }

    background_tasks.add_task(
        _run_ingest_pipeline, job_id, req.url, req.segment_duration, req.video_title
    )

    return {
        "job_id": job_id,
        "status": "processing",
        "status_url": f"/ingest-status?job_id={job_id}"
    }


@app.get("/ingest-status", dependencies=[Depends(verify_api_key)])
def ingest_status(job_id: str):
    """Consulta el estado de un job de ingesta."""
    if job_id not in ingest_jobs:
        raise HTTPException(status_code=404, detail=f"Ingest job {job_id} no encontrado")
    return ingest_jobs[job_id]


def _run_ingest_pipeline(job_id: str, url: str, segment_duration: int, video_title_override: str):
    """Background task: pipeline completo de ingesta."""
    job_dir = os.path.join(WORK_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    job = ingest_jobs[job_id]

    try:
        # --- Step 1: Metadata ---
        job["step"] = "downloading_metadata"
        meta_result = subprocess.run(
            YT_DLP_BASE + ["--dump-json", "--no-download", url],
            capture_output=True, text=True, timeout=60
        )
        if meta_result.returncode != 0:
            raise Exception(f"yt-dlp metadata: {meta_result.stderr[:500]}")
        meta = json.loads(meta_result.stdout)
        title = video_title_override or meta.get("title", "unknown")
        duration = meta.get("duration", 0)
        job["title"] = title

        # --- Step 2: Download ---
        job["step"] = "downloading_video"
        video_path = os.path.join(job_dir, "full_video.mp4")
        dl_result = subprocess.run(
            [
                *YT_DLP_BASE,
                "-f", "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best",
                "--merge-output-format", "mp4",
                "-o", video_path,
                url
            ],
            capture_output=True, text=True, timeout=600
        )
        if dl_result.returncode != 0:
            raise Exception(f"Download: {dl_result.stderr[:500]}")

        # --- Step 3: Segment ---
        job["step"] = "segmenting"
        segment_pattern = os.path.join(job_dir, "segment_%03d.mp4")
        seg_result = subprocess.run(
            [
                "ffmpeg", "-i", video_path,
                "-c", "copy", "-map", "0",
                "-segment_time", str(segment_duration),
                "-f", "segment",
                "-reset_timestamps", "1",
                segment_pattern
            ],
            capture_output=True, text=True, timeout=300
        )
        if seg_result.returncode != 0:
            raise Exception(f"Segmentation: {seg_result.stderr[:500]}")

        # Remove full video to free space
        if os.path.exists(video_path):
            os.remove(video_path)

        # --- Step 4: Embed & Store each segment ---
        segment_files = sorted(glob.glob(os.path.join(job_dir, "segment_*.mp4")))
        total = len(segment_files)
        job["step"] = "embedding"
        job["total_segments"] = total
        job["progress"] = f"0/{total}"

        for i, seg_path in enumerate(segment_files):
            job["progress"] = f"{i + 1}/{total}"
            job["embedded_segments"] = i

            start_sec = i * segment_duration
            end_sec = min(start_sec + segment_duration, duration)

            # Read segment as base64
            with open(seg_path, "rb") as f:
                b64_data = base64.b64encode(f.read()).decode("utf-8")

            # Call Gemini Embedding API
            embed_resp = http_requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-2-preview:embedContent?key={GOOGLE_API_KEY}",
                json={
                    "model": "models/gemini-embedding-2-preview",
                    "content": {
                        "parts": [{"inline_data": {"mime_type": "video/mp4", "data": b64_data}}]
                    },
                    "taskType": "RETRIEVAL_DOCUMENT",
                    "outputDimensionality": EMBEDDING_DIMENSIONS
                },
                timeout=180
            )

            if embed_resp.status_code != 200:
                raise Exception(f"Gemini embed segment {i}: {embed_resp.text[:500]}")

            embedding = embed_resp.json()["embedding"]["values"]

            # Free base64 memory
            del b64_data

            # Upsert to Qdrant
            point_id = str(uuid.uuid4())
            qdrant_resp = http_requests.put(
                f"{QDRANT_URL}:6333/collections/{QDRANT_COLLECTION}/points",
                headers={"api-key": QDRANT_API_KEY, "Content-Type": "application/json"},
                json={
                    "points": [{
                        "id": point_id,
                        "vector": embedding,
                        "payload": {
                            "video_title": title,
                            "video_url": url,
                            "segment_index": i,
                            "total_segments": total,
                            "start_time": f"{int(start_sec // 60):02d}:{int(start_sec % 60):02d}",
                            "end_time": f"{int(end_sec // 60):02d}:{int(end_sec % 60):02d}",
                            "start_seconds": start_sec,
                            "end_seconds": end_sec,
                            "duration_seconds": duration,
                            "job_id": job_id,
                            "ingested_at": datetime.utcnow().isoformat()
                        }
                    }]
                },
                timeout=30
            )

            if qdrant_resp.status_code not in [200, 201]:
                raise Exception(f"Qdrant segment {i}: {qdrant_resp.text[:500]}")

            # Delete segment file after successful embed to free disk
            os.remove(seg_path)

        # --- Step 5: Cleanup ---
        job["step"] = "cleanup"
        shutil.rmtree(job_dir, ignore_errors=True)

        job["status"] = "completed"
        job["step"] = "done"
        job["embedded_segments"] = total
        job["progress"] = f"{total}/{total}"
        job["completed_at"] = datetime.utcnow().isoformat()

    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)
        shutil.rmtree(job_dir, ignore_errors=True)


# === ORIGINAL ENDPOINTS (kept for flexibility) ===

@app.get("/health")
def health():
    yt_dlp = subprocess.run(["yt-dlp", "--version"], capture_output=True, text=True)
    ffmpeg = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True)
    return {
        "status": "ok",
        "service": "yt-video-processor",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "tools": {
            "yt-dlp": yt_dlp.stdout.strip() if yt_dlp.returncode == 0 else "NOT FOUND",
            "ffmpeg": "ok" if ffmpeg.returncode == 0 else "NOT FOUND"
        },
        "config": {
            "google_api_key": "configured" if GOOGLE_API_KEY else "MISSING",
            "qdrant_url": "configured" if QDRANT_URL else "MISSING",
            "qdrant_collection": QDRANT_COLLECTION,
            "embedding_dimensions": EMBEDDING_DIMENSIONS
        }
    }


@app.post("/process", dependencies=[Depends(verify_api_key)])
def process_video(req: ProcessRequest):
    """Descarga un video de YouTube y lo segmenta en chunks (sin embedding)."""
    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(WORK_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)

    try:
        meta_result = subprocess.run(
            YT_DLP_BASE + ["--dump-json", "--no-download", req.url],
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

    video_path = os.path.join(job_dir, "full_video.mp4")
    try:
        dl_result = subprocess.run(
            [
                *YT_DLP_BASE,
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

    segment_pattern = os.path.join(job_dir, "segment_%03d.mp4")
    try:
        seg_result = subprocess.run(
            [
                "ffmpeg", "-i", video_path,
                "-c", "copy", "-map", "0",
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

    if os.path.exists(video_path):
        os.remove(video_path)

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


@app.get("/segments", dependencies=[Depends(verify_api_key)])
def list_segments(job_id: str):
    """Lista los segmentos de un video procesado."""
    meta_path = os.path.join(WORK_DIR, job_id, "metadata.json")
    if not os.path.exists(meta_path):
        raise HTTPException(status_code=404, detail=f"Job {job_id} no encontrado")
    with open(meta_path) as f:
        return json.load(f)


@app.get("/download", dependencies=[Depends(verify_api_key)])
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


@app.get("/segment-base64", dependencies=[Depends(verify_api_key)])
def segment_base64(job_id: str, segment: int):
    """Devuelve un segmento en base64 listo para enviar a Gemini Embedding API."""
    seg_path = os.path.join(WORK_DIR, job_id, f"segment_{segment:03d}.mp4")
    if not os.path.exists(seg_path):
        raise HTTPException(status_code=404, detail=f"Segmento {segment} no encontrado en job {job_id}")

    file_size = os.path.getsize(seg_path)
    if file_size > 20 * 1024 * 1024:
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


@app.delete("/cleanup", dependencies=[Depends(verify_api_key)])
def cleanup(job_id: str):
    """Elimina todos los archivos de un job procesado."""
    job_dir = os.path.join(WORK_DIR, job_id)
    if not os.path.exists(job_dir):
        raise HTTPException(status_code=404, detail=f"Job {job_id} no encontrado")
    shutil.rmtree(job_dir)
    return {"status": "cleaned", "job_id": job_id}


@app.get("/jobs", dependencies=[Depends(verify_api_key)])
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
    # Also include active ingest jobs
    for jid, jdata in ingest_jobs.items():
        jobs.append({
            "job_id": jid,
            "title": jdata.get("title", ""),
            "type": "ingest",
            "status": jdata["status"],
            "progress": jdata["progress"]
        })
    return {"jobs": jobs}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
