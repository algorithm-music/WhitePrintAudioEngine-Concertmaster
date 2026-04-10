"""Tests for SSRF protection in job_conductor.validate_url_safe()."""

import pytest
from unittest.mock import patch
from concertmaster.services.job_conductor import validate_url_safe, _is_private_ip


# ── _is_private_ip ──

class TestIsPrivateIp:
    def test_loopback_v4(self):
        assert _is_private_ip("127.0.0.1") is True

    def test_loopback_v6(self):
        assert _is_private_ip("::1") is True

    def test_private_10(self):
        assert _is_private_ip("10.0.0.1") is True

    def test_private_172(self):
        assert _is_private_ip("172.16.0.1") is True

    def test_private_192(self):
        assert _is_private_ip("192.168.1.1") is True

    def test_link_local(self):
        assert _is_private_ip("169.254.169.254") is True

    def test_public(self):
        assert _is_private_ip("8.8.8.8") is False

    def test_invalid(self):
        assert _is_private_ip("not-an-ip") is False


# ── validate_url_safe ──

class TestValidateUrlSafe:
    def test_blocks_ftp_scheme(self):
        with pytest.raises(ValueError, match="Unsupported URL scheme"):
            validate_url_safe("ftp://example.com/file.wav")

    def test_blocks_file_scheme(self):
        with pytest.raises(ValueError, match="Unsupported URL scheme"):
            validate_url_safe("file:///etc/passwd")

    def test_blocks_metadata_hostname(self):
        with pytest.raises(ValueError, match="metadata service"):
            validate_url_safe("http://metadata.google.internal/computeMetadata/v1/")

    def test_blocks_metadata_short_hostname(self):
        with pytest.raises(ValueError, match="metadata service"):
            validate_url_safe("http://metadata/computeMetadata/v1/")

    def test_blocks_metadata_ip(self):
        with pytest.raises(ValueError, match="metadata service"):
            validate_url_safe("http://169.254.169.254/latest/meta-data/")

    @patch("concertmaster.services.job_conductor.socket.getaddrinfo")
    def test_blocks_private_resolved_ip(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("127.0.0.1", 443)),
        ]
        with pytest.raises(ValueError, match="private/internal"):
            validate_url_safe("http://evil.example.com/steal")

    @patch("concertmaster.services.job_conductor.socket.getaddrinfo")
    def test_blocks_internal_10_network(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("10.128.0.5", 443)),
        ]
        with pytest.raises(ValueError, match="private/internal"):
            validate_url_safe("http://sneaky.example.com/")

    @patch("concertmaster.services.job_conductor.socket.getaddrinfo")
    def test_allows_public_ip(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (2, 1, 6, "", ("151.101.1.69", 443)),
        ]
        # Should not raise
        validate_url_safe("https://cdn.example.com/track.wav")

    def test_blocks_unresolvable_hostname(self):
        with pytest.raises(ValueError, match="Cannot resolve"):
            validate_url_safe("https://this-host-does-not-exist-xyz123.invalid/file.wav")
