import socket

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.security import WebhookURLError, validate_webhook_url
from tests.conftest import DSN, KEY, submit

S = Settings(dsn=DSN, api_keys=(KEY,))


@pytest.fixture(autouse=True)
def fake_dns(monkeypatch):
    table = {"hooks.example.com": "93.184.216.34", "evil.example.com": "10.0.0.5", "meta.example.com": "169.254.169.254"}

    real = socket.getaddrinfo

    def fake(host, port, *a, **k):
        if host == "localhost":  # the test's own DB connection must still work
            return real(host, port, *a, **k)
        if host not in table:
            raise socket.gaierror("nope")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (table[host], port))]
    monkeypatch.setattr(socket, "getaddrinfo", fake)


@pytest.mark.parametrize("url", [
    "https://hooks.example.com/notify", "https://hooks.example.com:8443/x?y=1", "https://93.184.216.34/x",
])
def test_allows_public_https(url):
    assert validate_webhook_url(url, S) == url


@pytest.mark.parametrize("url", [
    "http://hooks.example.com/x",                # plain http
    "ftp://hooks.example.com/x", "file:///etc/passwd", "gopher://hooks.example.com/",
    "https://127.0.0.1/x", "https://[::1]/x", "https://localhost/x", "https://foo.localhost/x",
    "https://10.1.2.3/x", "https://192.168.0.10/x", "https://172.16.5.5/x",
    "https://169.254.169.254/latest/meta-data",  # cloud metadata
    "https://0.0.0.0/x", "https://[fe80::1]/x", "https://[::ffff:127.0.0.1]/x",
    "https://evil.example.com/x",                # DNS name -> private address
    "https://meta.example.com/x",                # DNS name -> link-local metadata address
    "https://user:pass@hooks.example.com/x",     # embedded credentials
    "https://nonexistent.example.com/x",         # does not resolve
    "https:///nohost", "not a url", "https://" + "a" * 3000 + ".com",
])
def test_rejects_dangerous(url):
    with pytest.raises(WebhookURLError):
        validate_webhook_url(url, S)


def test_allowlist_and_dev_overrides():
    strict = Settings(dsn=DSN, api_keys=(KEY,), webhook_allowed_hosts=("hooks.example.com",))
    validate_webhook_url("https://hooks.example.com/x", strict)
    with pytest.raises(WebhookURLError):
        validate_webhook_url("https://93.184.216.34/x", strict)
    dev = Settings(dsn=DSN, api_keys=(KEY,), webhook_allow_http=True, webhook_allow_private=True)
    validate_webhook_url("http://127.0.0.1:9000/hook", dev)


def test_api_rejects_ssrf_webhook_but_accepts_good_one(db):
    with TestClient(create_app(S), headers={"X-API-Key": KEY}) as c:
        assert submit(c, webhook_url="http://169.254.169.254/").status_code == 422
        assert submit(c, webhook_url="https://evil.example.com/x").status_code == 422
        r = submit(c, webhook_url="https://hooks.example.com/notify")
        assert r.status_code == 201 and r.json()["webhook_url"] == "https://hooks.example.com/notify"
