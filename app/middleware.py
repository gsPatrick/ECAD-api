import httpx
from fastapi import Cookie, HTTPException, Request, Depends
from typing import Optional
import os

# Configuration
# Ideally these should be in a settings file in the satellite system
CENTRAL_HUB_URL = os.getenv("CENTRAL_HUB_URL", "http://localhost:8000")
MY_SYSTEM_ID = os.getenv("MY_SYSTEM_ID", "1") # Change per satellite

async def get_current_user(request: Request):
    # Check for session cookie
    token = request.cookies.get("satellite_session")
    
    if not token:
        # Return 401 with JSON body instructing frontend where to redirect
        verify_url = f"{CENTRAL_HUB_URL}/auth/verify-session-browser?system_id={MY_SYSTEM_ID}&redirect_url={request.url}"
        raise HTTPException(
            status_code=401,
            detail={
                "message": "Session expired or invalid",
                "verify_url": verify_url
            }
        )
    
    return token

# For protecting routes
# Usage: @app.get("/protected", dependencies=[Depends(get_current_user)])
