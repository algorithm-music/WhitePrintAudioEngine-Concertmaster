"""
concertmaster — AI楽長（The Conductor）

The only externally-facing service. Orchestrates the full pipeline:
  audio_url → audition → deliberation → merge_rule → rendition_dsp → mastered WAV

API:
  POST /api/v1/jobs/master  — Submit mastering job
  GET  /health              — Liveness probe

Stores nothing. Remembers nothing. Returns everything.
"""

import json
import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, File, HTTPException, Request, Depends, Security, UploadFile
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from typing import Optional

import google.auth
from google.auth.transport.requests import Request as GAuthRequest

from concertmaster.clients.http_pool import init_pool, close_pool
from concertmaster.services import job_conductor

# ──────────────────────────────────────────
# Logging
# ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("concertmaster")

# ──────────────────────────────────────────
# Lifespan (shared HTTP pool)
# ──────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    yield
    await close_pool()


# ──────────────────────────────────────────
# App
# ──────────────────────────────────────────
app = FastAPI(
    title="concertmaster",
    description="AI-powered dynamic mastering. Paste a link. Get mastered audio.",
    version="1.0.0",
    docs_url="/docs",
    lifespan=lifespan,
)

# CORS — allow browser uploads from any origin (Vercel, localhost, etc)
_cors_origins = os.environ.get("CORS_ORIGINS", "").split(",")
_cors_origins = [o.strip() for o in _cors_origins if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins or ["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Request-Id", "X-Api-Key"],
)


# ──────────────────────────────────────────
# Request ID Middleware
# ──────────────────────────────────────────
@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    return response


# ──────────────────────────────────────────
# Request / Response Models
# ──────────────────────────────────────────
class MasterRequest(BaseModel):
    audio_url: Optional[str] = Field(
        None,
        description="Direct download URL. Use this OR input_path. URL path downloads via HTTP.",
        examples=["https://www.dropbox.com/s/abc123/track.wav?dl=1"],
    )
    input_path: Optional[str] = Field(
        None,
        description="Absolute path to input audio on the shared GCSFuse mount. Preferred — no HTTP transfer, no 32 MiB limits.",
        examples=["/mnt/gcs/aimastering-tmp-audio/uploads/user-123/abc.wav"],
    )
    output_path: Optional[str] = Field(
        None,
        description="Absolute path for the mastered output on the shared GCSFuse mount. When set, rendition-dsp writes directly (no HTTP response body).",
        examples=["/mnt/gcs/aimastering-tmp-audio/outputs/user-123/abc-master.wav"],
    )
    route: str = Field(
        "full",
        description="Pipeline route: full | analyze_only | deliberation_only | dsp_only",
    )
    target_lufs: float = Field(-14.0, description="Target integrated LUFS")
    target_true_peak: float = Field(-1.0, description="Target true peak dBTP")
    sage_config: Optional[dict] = Field(
        None,
        description="Sage vendor/model overrides for DELIBERATION",
    )
    dsp_config: Optional[dict] = Field(
        None,
        description="RENDITION_DSP chain config: {skip: [...], overrides: {...}}",
    )
    manual_params: Optional[dict] = Field(
        None,
        description="Manual RENDITION_DSP params (required for dsp_only route)",
    )
    output_url: Optional[str] = Field(
        None,
        description="Legacy: signed PUT URL for rendition-dsp to upload result. Prefer output_path.",
    )


# ──────────────────────────────────────────
# Authentication
# ──────────────────────────────────────────
API_KEY_NAME = "X-Api-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# SECURITY: No hardcoded fallback key. CONCERTMASTER_API_KEY env var is REQUIRED.
_api_key = os.environ.get("CONCERTMASTER_API_KEY")
if not _api_key:
    import warnings
    warnings.warn(
        "CONCERTMASTER_API_KEY is not set. All authenticated requests will be rejected.",
        RuntimeWarning,
        stacklevel=2,
    )
    VALID_API_KEYS: set[str] = set()
else:
    VALID_API_KEYS = {_api_key}

async def verify_api_key(api_key: str = Security(api_key_header)):
    if not VALID_API_KEYS or api_key not in VALID_API_KEYS:
        # User explicitly requested API keys. Block unauthorized access.
        raise HTTPException(
            status_code=401,
            detail="Unauthorized: Valid X-Api-Key header is required. None was provided or it was invalid.",
        )
    return api_key

# ──────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────
@app.get("/")
async def index():
    return {
        "status": "online",
        "service": "concertmaster",
        "engine": "WhitePrintAudioEngine",
        "documentation": "/docs"
    }

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "concertmaster",
        "stores_audio": False,
    }


# ──────────────────────────────────────────
# File Upload → GCS
# ──────────────────────────────────────────
GCS_SOURCE_BUCKET = os.environ.get("GCS_SOURCE_BUCKET", "aidriven-mastering-fyqu-source-bucket")
MAX_UPLOAD_SIZE = 200 * 1024 * 1024  # 200 MB

ALLOWED_AUDIO_TYPES = {
    "audio/wav", "audio/x-wav", "audio/wave",
    "audio/flac", "audio/x-flac",
    "audio/aiff", "audio/x-aiff",
    "audio/mpeg", "audio/mp3",
    "application/octet-stream",  # fallback for unknown mime
}


def _get_gcs_access_token() -> str:
    """Get GCS access token via Application Default Credentials (metadata server on Cloud Run)."""
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_write"]
    )
    credentials.refresh(GAuthRequest())
    return credentials.token


@app.post("/api/v1/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload audio file directly to GCS source bucket.

    Browsers call this endpoint directly (bypasses Vercel 4.5MB limit).
    Returns the public GCS URL for pipeline consumption.
    No API key required — files are temporary and auto-cleaned.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided.")

    # Validate content type
    content_type = (file.content_type or "application/octet-stream").lower()

    # Read file into memory
    data = await file.read()

    if len(data) > MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large: {len(data) / 1024 / 1024:.1f}MB. Max is {MAX_UPLOAD_SIZE / 1024 / 1024:.0f}MB.",
        )

    if len(data) < 44:
        raise HTTPException(status_code=400, detail="File too small to be valid audio.")

    # Generate unique object name
    timestamp = __import__("time").time()
    random_suffix = uuid.uuid4().hex[:8]
    import re
    safe_name = re.sub(r"[^a-zA-Z0-9._\-\u3000-\u9FFF\uF900-\uFAFF]", "_", file.filename)
    object_name = f"uploads/{int(timestamp)}-{random_suffix}/{safe_name}"

    # Upload to GCS using JSON API
    try:
        access_token = _get_gcs_access_token()

        upload_url = (
            f"https://storage.googleapis.com/upload/storage/v1/b/"
            f"{GCS_SOURCE_BUCKET}/o?uploadType=media&name={object_name}"
        )

        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                upload_url,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": content_type,
                },
                content=data,
            )

        if resp.status_code not in (200, 201):
            logger.error(f"[upload] GCS upload failed: {resp.status_code} {resp.text}")
            raise HTTPException(status_code=502, detail="Failed to upload file to storage.")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[upload] Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Upload error: {str(e)}")

    # Construct public URL
    from urllib.parse import quote
    gcs_url = f"https://storage.googleapis.com/{GCS_SOURCE_BUCKET}/{quote(object_name, safe='/')}"

    logger.info(f"[upload] Success: {file.filename} ({len(data) / 1024 / 1024:.1f}MB) → {gcs_url}")

    return {
        "url": gcs_url,
        "object_name": object_name,
        "file_name": file.filename,
        "file_size": len(data),
    }


@app.post("/api/v1/jobs/master")
async def master(req: MasterRequest, api_key: str = Depends(verify_api_key)):
    """Submit a mastering job.

    Paste a shared audio URL. Select a route. Get results.
    No upload. No storage. No account required.

    Routes:
      full:          analyze → deliberation → rendition_dsp → mastered WAV
      analyze_only:  analyze → analysis JSON
      deliberation_only:  analyze → deliberation → formplan JSON
      dsp_only:      rendition_dsp with manual_params → mastered WAV
    """
    route = req.route.lower().strip()

    if not req.input_path and not req.audio_url:
        raise HTTPException(
            status_code=400,
            detail="Either input_path (shared GCSFuse mount) or audio_url is required.",
        )

    # When the caller writes output to a shared path (or a signed PUT URL),
    # concertmaster responds with JSON metrics instead of the WAV bytes.
    output_is_out_of_band = bool(req.output_path or req.output_url)

    try:
        if route == "full":
            result = await job_conductor.run_full(
                audio_url=req.audio_url,
                input_path=req.input_path,
                output_path=req.output_path,
                target_lufs=req.target_lufs,
                target_true_peak=req.target_true_peak,
                sage_config=req.sage_config,
                dsp_config=req.dsp_config,
                output_url=req.output_url,
            )

            mastered = result.pop("mastered_audio", None)

            if output_is_out_of_band:
                return JSONResponse(content=result)
            else:
                return Response(
                    content=mastered,
                    media_type="audio/wav",
                    headers={
                        "X-Route": "full",
                        "X-Elapsed-Ms": str(result.get("elapsed_ms", 0)),
                        "X-Analysis": _safe_json_header(result.get("analysis", {})),
                        "X-Deliberation": _safe_json_header(result.get("deliberation", {})),
                        "X-Metrics": _safe_json_header(result.get("dsp_metrics", {})),
                    },
                )

        elif route == "analyze_only":
            result = await job_conductor.run_analyze_only(
                audio_url=req.audio_url,
                input_path=req.input_path,
            )
            return JSONResponse(content=result)

        elif route == "deliberation_only":
            result = await job_conductor.run_deliberation_only(
                audio_url=req.audio_url,
                input_path=req.input_path,
                target_lufs=req.target_lufs,
                target_true_peak=req.target_true_peak,
                sage_config=req.sage_config,
            )
            return JSONResponse(content=result)

        elif route == "dsp_only":
            if not req.manual_params:
                raise HTTPException(
                    status_code=400,
                    detail="manual_params required for dsp_only route",
                )
            result = await job_conductor.run_dsp_only(
                audio_url=req.audio_url,
                input_path=req.input_path,
                output_path=req.output_path,
                manual_params=req.manual_params,
                target_lufs=req.target_lufs,
                target_true_peak=req.target_true_peak,
                output_url=req.output_url,
            )

            mastered = result.pop("mastered_audio", None)

            if output_is_out_of_band:
                return JSONResponse(content=result)
            else:
                return Response(
                    content=mastered,
                    media_type="audio/wav",
                    headers={
                        "X-Route": "dsp_only",
                        "X-Elapsed-Ms": str(result.get("elapsed_ms", 0)),
                        "X-Metrics": _safe_json_header(result.get("dsp_metrics", {})),
                    },
                )

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown route: {route}. Use: full, analyze_only, deliberation_only, dsp_only",
            )

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.error("Pipeline timeout", exc_info=True)
        raise HTTPException(status_code=504, detail="Pipeline timed out. Please try again.")
    except httpx.HTTPStatusError as e:
        logger.error(f"Downstream service error: {e}", exc_info=True)
        raise HTTPException(
            status_code=502,
            detail=f"Downstream service returned {e.response.status_code}",
        )
    except Exception:
        logger.error("Pipeline failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal pipeline error")


# ──────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────
def _safe_json_header(data: dict, max_bytes: int = 4000) -> str:
    """Serialize dict for HTTP header (truncate if too large)."""
    try:
        raw = json.dumps(data, ensure_ascii=False, default=str)
        if len(raw.encode()) > max_bytes:
            return json.dumps({"truncated": True, "keys": list(data.keys())[:20]})
        return raw
    except Exception:
        return "{}"
