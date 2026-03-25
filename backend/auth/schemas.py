from datetime import datetime
from pydantic import BaseModel, EmailStr, field_validator
import re


class RegisterRequest(BaseModel):
    username: str
    email: EmailStr
    password: str
    tier: str = "free"

    @field_validator("username")
    @classmethod
    def username_valid(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 3 or len(v) > 64:
            raise ValueError("Username must be 3–64 characters")
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError("Username may only contain letters, digits, _ or -")
        return v

    @field_validator("password")
    @classmethod
    def password_strength(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v

    @field_validator("tier")
    @classmethod
    def tier_valid(cls, v: str) -> str:
        allowed = {"free", "basic", "pro", "admin"}
        if v not in allowed:
            raise ValueError(f"Tier must be one of {allowed}")
        return v


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int          # seconds until access_token expires


class RefreshRequest(BaseModel):
    refresh_token: str


class QuotaInfo(BaseModel):
    tokens_used: int
    token_limit: int
    year_month: str
    remaining: int
    percent_used: float


class UserMe(BaseModel):
    id: str
    username: str
    email: str
    tier: str
    is_active: bool
    created_at: datetime
    quota: QuotaInfo
