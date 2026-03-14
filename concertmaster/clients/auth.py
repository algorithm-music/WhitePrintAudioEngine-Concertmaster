"""
Service-to-service authentication for Cloud Run internal services.
Google公式パターン: OIDC ID token via default service account.
"""

import os
import google.auth.transport.requests
import google.oauth2.id_token


def get_auth_header(audience: str) -> dict:
    """Get Authorization header with OIDC token for internal service calls.

    On Cloud Run: uses default service account to mint ID token.
    Local dev: returns empty dict (no auth needed for local services).
    """
    if os.environ.get("K_SERVICE"):  # Running on Cloud Run
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
        return {"Authorization": f"Bearer {token}"}
    return {}  # Local development: no auth
