"""
URL Resolver — Uses Gemini to extract direct audio download URLs from arbitrary web pages.

For known providers (Google Drive, Dropbox, OneDrive, S3), uses deterministic URL rewriting.
For unknown URLs (Suno, SoundCloud, BandCamp, etc.), fetches the page HTML and asks Gemini
to find the direct audio stream URL.
"""

import os
import re
import logging
from urllib.parse import urlparse

import httpx
from google import genai

logger = logging.getLogger("concertmaster.url_resolver")

# Known providers that don't need Gemini
_KNOWN_AUDIO_HOSTS = {
    "drive.google.com",
    "docs.google.com",
    "dropbox.com",
    "www.dropbox.com",
    "dl.dropboxusercontent.com",
    "1drv.ms",
    "onedrive.live.com",
}

_GEMINI_MODEL = os.environ.get("URL_RESOLVER_MODEL", "gemini-2.5-flash-preview-05-20")

_SYSTEM_PROMPT = """You are an audio URL resolver. Given the HTML source of a web page that hosts audio content,
extract the direct download or streaming URL for the audio file (MP3, WAV, FLAC, AAC, OGG, M4A).

Look for:
- <audio> tags with src attributes
- <source> tags inside <audio> elements
- JavaScript variables containing CDN URLs (e.g., "https://cdn...mp3")
- Open Graph meta tags (og:audio, og:video with audio MIME types)
- JSON-LD or data attributes containing audio URLs
- API endpoints that return audio streams

Return ONLY the direct URL. No explanation. No markdown. Just the raw URL.
If multiple audio URLs are found, return the highest quality one (WAV > FLAC > MP3).
If no audio URL is found, return exactly: NO_AUDIO_FOUND"""


def is_known_provider(url: str) -> bool:
    """Check if the URL is from a known audio hosting provider."""
    try:
        host = urlparse(url).hostname or ""
        return any(known in host for known in _KNOWN_AUDIO_HOSTS)
    except Exception:
        return False


def _looks_like_direct_audio(url: str) -> bool:
    """Check if URL likely points directly to an audio file."""
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in ('.wav', '.flac', '.aiff', '.aif', '.mp3', '.ogg', '.m4a', '.aac'))


async def resolve_audio_url(url: str) -> str:
    """Resolve an arbitrary URL to a direct audio download URL.

    For known providers, returns the URL as-is (handled by normalize_audio_url).
    For unknown URLs, fetches the page and uses Gemini to extract the audio URL.

    Returns:
        Direct audio URL string

    Raises:
        ValueError: If no audio URL could be found
    """
    # Skip resolution for known providers and direct audio links
    if is_known_provider(url) or _looks_like_direct_audio(url):
        return url

    logger.info(f"Unknown audio source, resolving with Gemini: {url}")

    # Fetch the page HTML
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
            resp = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            resp.raise_for_status()
            html = resp.text
    except Exception as e:
        raise ValueError(f"Failed to fetch page at {url}: {e}")

    # Truncate HTML to avoid token limits (keep first 30k chars)
    html_truncated = html[:30000]

    # Ask Gemini to extract the audio URL
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY not set — cannot resolve unknown audio URLs")

    try:
        gemini = genai.Client(api_key=api_key)
        response = gemini.models.generate_content(
            model=_GEMINI_MODEL,
            contents=[f"Extract the direct audio URL from this page HTML:\n\nURL: {url}\n\nHTML:\n{html_truncated}"],
            config=genai.types.GenerateContentConfig(
                system_instruction=_SYSTEM_PROMPT,
                temperature=0.0,
            ),
        )
        result = (response.text or "").strip()
    except Exception as e:
        raise ValueError(f"Gemini URL resolution failed: {e}")

    if not result or result == "NO_AUDIO_FOUND":
        raise ValueError(
            f"No audio URL found on page: {url}. "
            "Please provide a direct link to a WAV, FLAC, or AIFF file, "
            "or use Google Drive / Dropbox / S3."
        )

    # Validate the extracted URL
    extracted = result.split('\n')[0].strip()  # Take first line only
    if not extracted.startswith("http"):
        raise ValueError(f"Gemini returned invalid URL: {extracted}")

    logger.info(f"Resolved audio URL: {extracted[:80]}...")
    return extracted
