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
from fastapi import FastAPI, HTTPException, Request, Depends, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from typing import Optional

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

# CORS
_cors_origins = os.environ.get("CORS_ORIGINS", "").split(",")
_cors_origins = [o.strip() for o in _cors_origins if o.strip()]
if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
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
    audio_url: str = Field(
        ...,
        description="Direct download URL (Dropbox, Google Drive, OneDrive, S3, any HTTPS)",
        examples=["https://www.dropbox.com/s/abc123/track.wav?dl=1"],
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
        description="Signed PUT URL to upload result directly (bypasses memory and HTTP return size limits)",
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

    try:
        if route == "full":
            result = await job_conductor.run_full(
                audio_url=req.audio_url,
                target_lufs=req.target_lufs,
                target_true_peak=req.target_true_peak,
                sage_config=req.sage_config,
                dsp_config=req.dsp_config,
                output_url=req.output_url,
            )
            
            mastered = result.pop("mastered_audio", None)
            
            if req.output_url:
                # Result was uploaded to storage, return JSON
                return JSONResponse(content=result)
            else:
                # Return mastered WAV as binary response
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
            )
            return JSONResponse(content=result)

        elif route == "deliberation_only":
            result = await job_conductor.run_deliberation_only(
                audio_url=req.audio_url,
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
                manual_params=req.manual_params,
                target_lufs=req.target_lufs,
                target_true_peak=req.target_true_peak,
                output_url=req.output_url,
            )
            
            mastered = result.pop("mastered_audio", None)
            
            if req.output_url:
                # Result was uploaded to storage, return JSON
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
