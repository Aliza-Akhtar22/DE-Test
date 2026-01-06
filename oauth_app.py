import os
import secrets
import urllib.parse
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse
import requests

app = FastAPI()

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPE = "https://www.googleapis.com/auth/adwords"

STATE = {}

def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


@app.get("/start")
def start():
    state = secrets.token_urlsafe(24)
    STATE["state"] = state

    params = {
        "client_id": env("GOOGLE_CLIENT_ID"),
        "redirect_uri": env("GOOGLE_CALLBACK_URL"),
        "response_type": "code",
        "scope": SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }

    url = AUTH_URL + "?" + urllib.parse.urlencode(params)
    return RedirectResponse(url)


@app.get("/api/oauth/google/callback")
def callback(request: Request):
    if request.query_params.get("state") != STATE.get("state"):
        return HTMLResponse("Invalid state", status_code=400)

    code = request.query_params.get("code")
    if not code:
        return HTMLResponse("Missing code", status_code=400)

    data = {
        "client_id": env("GOOGLE_CLIENT_ID"),
        "client_secret": env("GOOGLE_CLIENT_SECRET"),
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": env("GOOGLE_CALLBACK_URL"),
    }

    r = requests.post(TOKEN_URL, data=data)
    token = r.json()

    return HTMLResponse(
        "<h2>OAuth success</h2>"
        f"<pre>{token}</pre>"
        "<p>Copy the <b>refresh_token</b> into .dlt/secrets.toml</p>"
    )
  