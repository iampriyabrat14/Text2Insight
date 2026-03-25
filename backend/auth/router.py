"""
Auth router — /auth/register, /auth/login, /auth/refresh, /auth/me, /auth/logout
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.auth import schemas
from backend.auth import service
from backend.auth.token_ledger import get_quota_info
from backend.database.sqlite_manager import get_db
from backend.dependencies import get_current_user
from backend.database.sqlite_manager import User

router = APIRouter()


@router.post("/register", status_code=status.HTTP_403_FORBIDDEN)
async def register():
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Registration is disabled")


@router.post("/login", response_model=schemas.TokenResponse)
async def login(body: schemas.LoginRequest, db: AsyncSession = Depends(get_db)):
    user = await service.authenticate_user(db, body.username, body.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token, expires_in = service.create_access_token(user.id, user.username, user.tier)
    refresh_token = service.create_refresh_token_value()
    await service.store_refresh_token(db, user.id, refresh_token)

    return schemas.TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=expires_in,
    )


@router.post("/refresh", response_model=schemas.TokenResponse)
async def refresh(body: schemas.RefreshRequest, db: AsyncSession = Depends(get_db)):
    user = await service.validate_and_rotate_refresh_token(db, body.refresh_token)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token invalid or expired",
        )

    access_token, expires_in = service.create_access_token(user.id, user.username, user.tier)
    new_refresh_token = service.create_refresh_token_value()
    await service.store_refresh_token(db, user.id, new_refresh_token)

    return schemas.TokenResponse(
        access_token=access_token,
        refresh_token=new_refresh_token,
        expires_in=expires_in,
    )


@router.get("/me", response_model=schemas.UserMe)
async def me(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    quota = await get_quota_info(db, user)
    return schemas.UserMe(
        id=user.id,
        username=user.username,
        email=user.email,
        tier=user.tier,
        is_active=user.is_active,
        created_at=user.created_at,
        quota=schemas.QuotaInfo(**quota),
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await service.revoke_all_refresh_tokens(db, user.id)
