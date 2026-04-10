"""Tests for URL normalization in job_conductor.normalize_audio_url()."""

import pytest
from concertmaster.services.job_conductor import normalize_audio_url


class TestNormalizeGoogleDrive:
    def test_file_view_url(self):
        url = "https://drive.google.com/file/d/1aBcDeFgHiJkLmNoPqRsT/view"
        result = normalize_audio_url(url)
        assert result == "https://drive.google.com/uc?export=download&id=1aBcDeFgHiJkLmNoPqRsT"

    def test_file_view_with_query(self):
        url = "https://drive.google.com/file/d/ABC123/view?usp=sharing"
        result = normalize_audio_url(url)
        assert result == "https://drive.google.com/uc?export=download&id=ABC123"

    def test_file_edit_url(self):
        url = "https://drive.google.com/file/d/XYZ789/edit"
        result = normalize_audio_url(url)
        assert result == "https://drive.google.com/uc?export=download&id=XYZ789"


class TestNormalizeDropbox:
    def test_dl_0_to_dl_1(self):
        url = "https://www.dropbox.com/s/abc123/track.wav?dl=0"
        result = normalize_audio_url(url)
        assert "dl=1" in result
        assert "dl=0" not in result

    def test_no_dl_param(self):
        url = "https://www.dropbox.com/s/abc123/track.wav"
        result = normalize_audio_url(url)
        assert "dl=1" in result

    def test_preserves_other_params(self):
        url = "https://www.dropbox.com/s/abc123/track.wav?dl=0&foo=bar"
        result = normalize_audio_url(url)
        assert "dl=1" in result
        assert "foo=bar" in result


class TestNormalizeOneDrive:
    def test_1drv_ms(self):
        url = "https://1drv.ms/u/s!AaBbCcDdEe"
        result = normalize_audio_url(url)
        assert result.endswith("?download=1")

    def test_onedrive_live(self):
        url = "https://onedrive.live.com/redir?resid=ABC"
        result = normalize_audio_url(url)
        assert result.endswith("&download=1")


class TestNormalizePassthrough:
    def test_s3_signed_url(self):
        url = "https://my-bucket.s3.amazonaws.com/track.wav?X-Amz-Signature=abc"
        result = normalize_audio_url(url)
        assert result == url

    def test_generic_https(self):
        url = "https://cdn.example.com/audio/track.wav"
        result = normalize_audio_url(url)
        assert result == url
