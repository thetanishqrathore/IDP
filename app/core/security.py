from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from core.config import settings
import os

API_KEY_NAME = "Authorization"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

IDP_API_KEY = settings.idp_api_key

async def get_api_key(api_key_header: str = Security(api_key_header)):
    """
    Validates the Bearer token.
    If IDP_API_KEY is not set in env, we default to allowing access (DEV mode),
    but in PROD you must set it.
    """
    if not IDP_API_KEY:
        # Open mode (Development)
        return True

    if not api_key_header:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials"
        )
    
    # robust parsing of "Bearer <token>"
    try:
        scheme, _, token = api_key_header.partition(" ")
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid auth format"
        )
        
    if scheme.lower() != "bearer" or token != IDP_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API Key"
        )
    return token
