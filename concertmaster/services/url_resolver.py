"""
URL Resolver — Resolves arbitrary audio URLs to direct download URLs or local files.

Resolution chain:
1. Known providers (Google Drive, Dropbox, OneDrive, S3) → URL rewrite
2. Direct audio links (.wav/.mp3/.flac) → pass through
3. Suno special: extract cdn1.suno.ai direct link from og:audio meta tag
4. yt-dlp extract_info (get direct URL without downloading)
5. yt-dlp download to local WAV (last resort)
6. Gemini HTML scraping (final fallback)

Returns either:
  {"type": "url", "value": "https://cdn1.suno.ai/..."} — direct URL
  {"type": "file", "value": "/tmp/ytdlp_xxx/audio.wav"} — local file path
"""

import asyncio
import json
import os
import re
import logging
import subprocess
import tempfile
from urllib.parse import urlparse

import httpx

logger = logging.getLogger("concertmaster.url_resolver")

_KNOWN_AUDIO_HOSTS = {
    "drive.google.com", "docs.google.com",
    "dropbox.com", "www.dropbox.com", "dl.dropboxusercontent.com",
    "1drv.ms", "onedrive.live.com",
}


def is_known_provider(url: str) -> bool:
    try:
        host = urlparse(url).hostname or ""
        return any(known in host for known in _KNOWN_AUDIO_HOSTS)
    except Exception:
        return False


def _looks_like_direct_audio(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in (
        '.wav', '.flac', '.aiff', '.aif', '.mp3', '.ogg', '.m4a', '.aac',
    ))


async def resolve_audio_url(url: str) -> dict:
    """Resolve an arbitrary URL to audio data.

    Returns:
        {"type": "url", "value": "https://..."} or
        {"type": "file", "value": "/tmp/.../audio.wav"}
    """
    if _looks_like_direct_audio(url):
        return {"type": "url", "value": url}

    # Google Drive: extract file ID → direct download → FFmpeg → WAV
    if "drive.google.com" in url or "docs.google.com" in url:
        try:
            local_wav = await _download_gdrive(url)
            if local_wav:
                return {"type": "file", "value": local_wav}
        except Exception as e:
            logger.warning(f"GDrive download failed: {e}")

    if is_known_provider(url):
        return {"type": "url", "value": url}

    logger.info(f"Unknown audio source, resolving: {url}")

    # 1. Suno special — extract CDN link, download & convert to WAV
    if "suno.com" in url:
        try:
            local_wav = await _resolve_suno(url)
            if local_wav:
                return {"type": "file", "value": local_wav}
        except Exception as e:
            logger.warning(f"Suno direct extraction failed: {e}")

    # 2. yt-dlp extract_info (get URL without downloading)
    try:
        direct = await _ytdlp_extract_url(url)
        if direct:
            return {"type": "url", "value": direct}
    except Exception as e:
        logger.info(f"yt-dlp URL extraction failed: {e}")

    # 3. yt-dlp download to local WAV
    try:
        local_path = await _ytdlp_download(url)
        if local_path:
            return {"type": "file", "value": local_path}
    except Exception as e:
        logger.info(f"yt-dlp download failed: {e}")

    # 4. Gemini HTML scraping (final fallback)
    try:
        direct = await _resolve_with_gemini(url)
        if direct:
            return {"type": "url", "value": direct}
    except Exception as e:
        logger.warning(f"Gemini resolution failed: {e}")

    raise ValueError(
        f"Cannot resolve audio from: {url}. "
        "Tried: Suno CDN, yt-dlp, Gemini. "
        "Please provide a direct audio URL or Google Drive/Dropbox link."
    )


async def _download_gdrive(url: str) -> str | None:
    """Download from Google Drive with virus scan bypass + FFmpeg WAV conversion."""
    # Extract file ID
    match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', url)
    if not match:
        match = re.search(r'id=([a-zA-Z0-9_-]+)', url)
    if not match:
        logger.warning("Cannot extract file ID from GDrive URL")
        return None

    file_id = match.group(1)
    dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    logger.info(f"GDrive direct download: file_id={file_id}")

    fd_raw, temp_raw = tempfile.mkstemp(suffix=".tmp")
    os.close(fd_raw)
    fd_wav, temp_wav = tempfile.mkstemp(suffix=".wav")
    os.close(fd_wav)

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=120.0) as client:
            resp = await client.get(dl_url)

            # Bypass virus scan warning (large files)
            confirm_token = None
            for key, value in resp.cookies.items():
                if key.startswith("download_warning"):
                    confirm_token = value
                    break

            if confirm_token:
                logger.info("Bypassing GDrive virus scan...")
                resp = await client.get(f"{dl_url}&confirm={confirm_token}")

            resp.raise_for_status()

            with open(temp_raw, "wb") as f:
                f.write(resp.content)

        sz = os.path.getsize(temp_raw)
        if sz < 50000:
            raise ValueError(f"GDrive file too small ({sz}B) — likely HTML error page")

        logger.info(f"GDrive downloaded {sz / 1024:.0f}KB, converting to WAV...")

        await asyncio.to_thread(
            subprocess.run,
            ["ffmpeg", "-y", "-i", temp_raw,
             "-vn", "-acodec", "pcm_s24le", "-ar", "48000",
             temp_wav],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        wav_sz = os.path.getsize(temp_wav)
        logger.info(f"GDrive WAV: {wav_sz / 1024 / 1024:.1f}MB")
        return temp_wav

    except Exception as e:
        if os.path.exists(temp_wav):
            os.remove(temp_wav)
        raise RuntimeError(f"GDrive failed: {e}")
    finally:
        if os.path.exists(temp_raw):
            os.remove(temp_raw)


async def _resolve_suno(url: str) -> str | None:
    """Extract CDN link from Suno, download MP3, convert to WAV."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
        resp = await client.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        resp.raise_for_status()

        direct_url = None
        for pattern in [
            r'property="og:audio"\s+content="([^"]+)"',
            r'content="([^"]*cdn[^"]*\.(?:mp3|wav|m4a|ogg))"',
            r'"audio_url"\s*:\s*"([^"]+)"',
            r'"download_url"\s*:\s*"([^"]+)"',
            r'"mp3_url"\s*:\s*"([^"]+)"',
        ]:
            match = re.search(pattern, resp.text)
            if match and match.group(1).startswith("http"):
                direct_url = match.group(1)
                break

    if not direct_url:
        return None

    logger.info(f"Suno CDN found: {direct_url[:80]}...")
    return await _download_and_convert_to_wav(direct_url)


async def _download_and_convert_to_wav(direct_url: str) -> str:
    """Download audio from CDN and convert to WAV via FFmpeg."""
    fd_src, temp_src = tempfile.mkstemp(suffix=".mp3")
    os.close(fd_src)
    fd_wav, temp_wav = tempfile.mkstemp(suffix=".wav")
    os.close(fd_wav)

    try:
        # Download with browser-like headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://suno.com/",
        }
        async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
            async with client.stream("GET", direct_url, headers=headers) as resp:
                resp.raise_for_status()
                with open(temp_src, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=8192):
                        f.write(chunk)

        src_size = os.path.getsize(temp_src)
        if src_size < 10000:
            raise ValueError(f"Downloaded file too small ({src_size}B)")

        logger.info(f"Downloaded {src_size / 1024:.0f}KB, converting to WAV...")

        # Convert to 24bit/48kHz WAV via FFmpeg
        await asyncio.to_thread(
            subprocess.run,
            ["ffmpeg", "-y", "-i", temp_src,
             "-vn", "-acodec", "pcm_s24le", "-ar", "48000",
             temp_wav],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        wav_size = os.path.getsize(temp_wav)
        logger.info(f"WAV converted: {wav_size / 1024 / 1024:.1f}MB")
        return temp_wav

    except Exception as e:
        if os.path.exists(temp_wav):
            os.remove(temp_wav)
        raise RuntimeError(f"Download/convert failed: {e}")
    finally:
        if os.path.exists(temp_src):
            os.remove(temp_src)


async def _ytdlp_extract_url(url: str) -> str | None:
    """Use yt-dlp to get direct streaming URL without downloading."""
    try:
        import yt_dlp
    except ImportError:
        return None

    def _extract():
        with yt_dlp.YoutubeDL({"quiet": True, "skip_download": True}) as ydl:
            info = ydl.extract_info(url, download=False)
            if info and "url" in info:
                return info["url"]
            if info and "formats" in info:
                # Find best audio format
                audio_fmts = [
                    f for f in info["formats"]
                    if f.get("acodec") != "none" and f.get("url")
                ]
                if audio_fmts:
                    best = max(audio_fmts, key=lambda f: f.get("abr", 0) or 0)
                    return best["url"]
        return None

    result = await asyncio.to_thread(_extract)
    if result:
        logger.info(f"yt-dlp URL extracted: {result[:80]}...")
    return result


async def _ytdlp_download(url: str) -> str | None:
    """Download audio to local WAV via yt-dlp + ffmpeg."""
    try:
        import yt_dlp
    except ImportError:
        return None

    tmp_dir = tempfile.mkdtemp(prefix="ytdlp_")
    out_template = os.path.join(tmp_dir, "audio.%(ext)s")

    def _download():
        with yt_dlp.YoutubeDL({
            "format": "bestaudio/best",
            "outtmpl": out_template,
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "wav",
            }],
            "quiet": True,
            "no_warnings": True,
        }) as ydl:
            ydl.download([url])

    await asyncio.to_thread(_download)

    for f in os.listdir(tmp_dir):
        if f.endswith(".wav"):
            path = os.path.join(tmp_dir, f)
            size = os.path.getsize(path)
            if size < 50000:  # 50KB = likely HTML, not audio
                logger.warning(f"yt-dlp file too small ({size}B), likely not audio")
                os.remove(path)
                return None
            logger.info(f"yt-dlp downloaded: {path} ({size / 1024:.0f}KB)")
            return path

    return None


async def _resolve_with_gemini(url: str) -> str | None:
    """Fetch page HTML and ask Gemini to find the audio URL."""
    from google import genai

    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        return None

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
            resp = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            })
            resp.raise_for_status()
            html = resp.text[:30000]
    except Exception:
        return None

    gemini = genai.Client(api_key=api_key)
    model = os.environ.get("URL_RESOLVER_MODEL", "gemini-2.5-flash-preview-05-20")

    response = gemini.models.generate_content(
        model=model,
        contents=[
            f"Extract the direct audio URL from this HTML:\n\nURL: {url}\n\nHTML:\n{html}"
        ],
        config=genai.types.GenerateContentConfig(
            system_instruction=(
                "Return ONLY the direct audio URL (mp3/wav/flac/m4a). "
                "No explanation. If not found, return NO_AUDIO_FOUND"
            ),
            temperature=0.0,
        ),
    )
    result = (response.text or "").strip().split("\n")[0]
    if result and result != "NO_AUDIO_FOUND" and result.startswith("http"):
        logger.info(f"Gemini resolved: {result[:80]}...")
        return result
    return None
