"""Minimal live-auth verification test for the Braze connector.

Reads credentials from CONNECTOR_TEST_CONFIG_PATH (or CONNECTOR_TEST_CONFIG_JSON),
constructs a Bearer-auth header, and makes ONE GET request to a cheap list
endpoint to confirm the key + endpoint are valid.

Run:
    CONNECTOR_TEST_CONFIG_PATH=tests/unit/sources/braze/configs/dev_config.json \
        pytest tests/unit/sources/braze/test_braze_auth_verify.py -v

PASS  => HTTP 200, auth valid.
FAIL  => HTTP 401/403 (bad key / missing permission) or other error.
"""

from __future__ import annotations

import os
import urllib.request
import urllib.error
import json

import pytest

from tests.unit.sources.test_utils import load_config

# Live-only: this test makes a real HTTP call to the Braze REST API, so it runs
# only when live credentials are supplied. In the default (offline / simulate)
# run — e.g. CI — no config is present and the test skips cleanly.
_HAS_LIVE_CONFIG = bool(
    os.environ.get("CONNECTOR_TEST_CONFIG_PATH")
    or os.environ.get("CONNECTOR_TEST_CONFIG_JSON")
)


@pytest.mark.skipif(
    not _HAS_LIVE_CONFIG,
    reason="live auth check: set CONNECTOR_TEST_CONFIG_PATH or "
    "CONNECTOR_TEST_CONFIG_JSON to run",
)
def test_braze_bearer_auth() -> None:
    """Verify that the configured API key is accepted by the Braze REST API."""
    config = load_config()
    rest_endpoint = config["rest_endpoint"].rstrip("/")
    api_key = config["api_key"]

    url = f"{rest_endpoint}/campaigns/list?page=0"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {api_key}"},
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
            body = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        status = exc.code
        try:
            body = json.loads(exc.read())
        except Exception:
            body = {}
        error_msg = body.get("message", "(no message)")
        pytest.fail(
            f"FAIL — HTTP {status}: {error_msg}  "
            f"(url={url})"
        )

    assert status == 200, (
        f"FAIL — expected HTTP 200 but got {status}; body={body}"
    )
    # A valid response has a 'campaigns' key (may be an empty list).
    assert "campaigns" in body, (
        f"FAIL — HTTP 200 but unexpected body shape (missing 'campaigns'): {body}"
    )
    print(
        f"\nPASS — HTTP {status}, campaigns returned: {len(body.get('campaigns', []))}"
    )
