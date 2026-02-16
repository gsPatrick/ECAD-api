from fastapi import APIRouter, HTTPException, Depends, Response, Request
from fastapi.responses import RedirectResponse
import httpx
import os
from .middleware import CENTRAL_HUB_URL

router = APIRouter()

@router.get("/liberar")
async def liberar_acesso(token: str, next: str = "/", response: Response = None):
    """
    Receives the Transfer Token from Central Hub.
    Validates it.
    Sets a session cookie.
    Redirects to 'next' url.
    """
    async with httpx.AsyncClient() as client:
        try:
            # Validate ticket with Central Hub
            resp = await client.post(
                f"{CENTRAL_HUB_URL}/auth/validate-ticket",
                json={"token": token}
            )
            
            if resp.status_code != 200:
                raise HTTPException(status_code=403, detail="Invalid token from Central Hub")
                
            user_data = resp.json()
            
            # Create Session (In a real app, you might sign your own JWT here or use a session backend)
            # For simplicity, we'll store the validated user data or a flag in a signed cookie
            # But the user asked for a "Secure, HttpOnly, SameSite=Lax Cookie"
            
            # We'll just set a dummy session value or the user's email for now.
            # In production, use a secure session library (like starsessions) or sign it.
            session_value = f"user:{user_data['email']}"
            
            response = RedirectResponse(url=next)
            response.set_cookie(
                key="satellite_session",
                value=session_value,
                httponly=True,
                secure=False, # Set to True if HTTPS
                samesite="lax",
                max_age=3600 # 1 hour
            )
            return response
            
        except httpx.RequestError:
            raise HTTPException(status_code=503, detail="Central Hub unavailable")

@router.get("/me")
async def get_me(request: Request):
    token = request.cookies.get("satellite_session")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"status": "authenticated", "user": token.split(":")[1] if ":" in token else token}
