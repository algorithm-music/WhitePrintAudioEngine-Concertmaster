import os
import pytest

# Set required env vars before importing app modules
os.environ.setdefault("CONCERTMASTER_API_KEY", "test-key-12345")
os.environ.setdefault("AUDITION_URL", "http://audition:8081")
os.environ.setdefault("DELIBERATION_URL", "http://deliberation:8082")
os.environ.setdefault("RENDITION_DSP_URL", "http://rendition-dsp:8083")
