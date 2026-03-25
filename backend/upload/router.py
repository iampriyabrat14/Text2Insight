from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.sqlite_manager import User, get_db
from backend.dependencies import get_current_user
from backend.upload.service import process_csv_upload, list_user_uploads, drop_upload

router = APIRouter()


@router.post("/csv")
async def upload_csv(
    file: UploadFile = File(...),
    session_id: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only .csv files are accepted.",
        )
    try:
        result = await process_csv_upload(db, user, file, session_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return result


@router.delete("/{upload_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_upload(
    upload_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    deleted = await drop_upload(db, user, upload_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload not found")


@router.get("/list")
async def list_uploads(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await list_user_uploads(db, user)
