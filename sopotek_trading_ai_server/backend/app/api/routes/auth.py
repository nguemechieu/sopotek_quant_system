from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import (
    create_access_token,
    create_password_reset_token,
    decode_password_reset_token,
    get_current_user,
    get_db,
    hash_password,
    verify_password,
)
from app.models.enums import UserRole
from app.models.user import User
from app.schemas.auth import (
    ForgotPasswordRequest,
    ForgotPasswordResponse,
    LoginRequest,
    RegisterRequest,
    ResetPasswordRequest,
    TokenResponse,
    UserResponse,
)
from app.services.bootstrap import provision_user_defaults


router = APIRouter()


@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(
    payload: RegisterRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    existing = await db.scalar(
        select(User).where(or_(User.email == payload.email.lower(), User.username == payload.username.lower()))
    )
    if existing is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists")

    user_count = int(await db.scalar(select(func.count(User.id))) or 0)
    requested_role = payload.role or (UserRole.ADMIN if user_count == 0 else UserRole.TRADER)
    user = User(
        email=payload.email.lower(),
        username=payload.username.lower(),
        full_name=payload.full_name,
        password_hash=hash_password(payload.password),
        role=requested_role,
        is_active=True,
    )
    db.add(user)
    await db.flush()
    await provision_user_defaults(db, user)
    await db.commit()
    await db.refresh(user)

    token = create_access_token(subject=user.id, role=user.role.value, settings=request.app.state.settings)
    return TokenResponse(access_token=token, user=UserResponse.model_validate(user))


@router.post("/login", response_model=TokenResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    user = await db.scalar(select(User).where(User.email == payload.email.lower()))
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")
    token = create_access_token(subject=user.id, role=user.role.value, settings=request.app.state.settings)
    return TokenResponse(access_token=token, user=UserResponse.model_validate(user))


@router.post("/forgot-password", response_model=ForgotPasswordResponse)
async def forgot_password(
    payload: ForgotPasswordRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> ForgotPasswordResponse:
    settings = request.app.state.settings
    user = await db.scalar(select(User).where(User.email == payload.email.lower()))
    response = ForgotPasswordResponse(
        message="If that account exists, a password reset link has been prepared."
    )
    if user is None:
        return response

    reset_token = create_password_reset_token(subject=user.id, settings=settings)
    if settings.environment.lower() != "production":
        reset_url = f"{settings.frontend_base_url.rstrip('/')}/reset-password?token={reset_token}"
        response.reset_token = reset_token
        response.reset_url = reset_url
    return response


@router.post("/reset-password", response_model=TokenResponse)
async def reset_password(
    payload: ResetPasswordRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    token_payload = decode_password_reset_token(payload.token, request.app.state.settings)
    subject = str(token_payload.get("sub") or "").strip()
    if not subject:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid password reset token")

    user = await db.scalar(select(User).where(User.id == subject))
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found or inactive")

    user.password_hash = hash_password(payload.password)
    await db.commit()
    await db.refresh(user)

    access_token = create_access_token(subject=user.id, role=user.role.value, settings=request.app.state.settings)
    return TokenResponse(access_token=access_token, user=UserResponse.model_validate(user))


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)) -> UserResponse:
    return UserResponse.model_validate(current_user)
