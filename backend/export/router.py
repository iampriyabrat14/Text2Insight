from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.sqlite_manager import User, get_db
from backend.dependencies import get_current_user
from backend.export.service import build_export

router = APIRouter()


@router.get("/{session_id}")
async def export_session(
    session_id: str,
    format: str = Query("pdf", description="Export format: pdf | word | ppt"),
    message_id: str | None = Query(None, description="Export only this message"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await build_export(db, user, session_id, format.lower(), message_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")

    file_bytes, filename, mime = result
    return Response(
        content=file_bytes,
        media_type=mime,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
