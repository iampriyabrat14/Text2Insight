import json
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from backend.chat import schemas, service
from backend.database.sqlite_manager import User, get_db
from backend.middleware.rate_limiter import rate_limit

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/stream")
async def stream_query(
    body: schemas.QueryRequest,
    user: User = Depends(rate_limit),
    db: AsyncSession = Depends(get_db),
):
    """
    SSE endpoint — streams SQL, query result, and summary tokens in real time.
    Event types: status | sql | result | token | done | error
    """
    async def _generate():
        try:
            async for chunk in service.run_query_pipeline_stream(
                db, user, body.query, body.session_id, body.upload_table,
            ):
                yield chunk
        except PermissionError as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"
        except Exception as exc:
            logger.exception("Unhandled stream error")
            yield f"data: {json.dumps({'type': 'error', 'message': 'An unexpected error occurred.'})}\n\n"

    return StreamingResponse(
        _generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # prevent nginx/proxy from buffering SSE
        },
    )


@router.post("/query", response_model=schemas.QueryResponse)
async def query(
    body: schemas.QueryRequest,
    user: User = Depends(rate_limit),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await service.run_query_pipeline(db, user, body.query, body.session_id, body.upload_table)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
    return result


@router.get("/sessions", response_model=list[schemas.SessionOut])
async def list_sessions(
    user: User = Depends(rate_limit),
    db: AsyncSession = Depends(get_db),
):
    return await service.list_sessions(db, user)


@router.get("/sessions/{session_id}", response_model=schemas.SessionDetail)
async def get_session(
    session_id: str,
    user: User = Depends(rate_limit),
    db: AsyncSession = Depends(get_db),
):
    data = await service.get_session_messages(db, user, session_id)
    if data is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    return data


@router.post("/sessions/{session_id}/rename", status_code=status.HTTP_204_NO_CONTENT)
async def rename_session(
    session_id: str,
    body: schemas.RenameRequest,
    user: User = Depends(rate_limit),
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy import select, update
    from backend.database.sqlite_manager import ChatSession
    result = await db.execute(
        select(ChatSession).where(ChatSession.id == session_id, ChatSession.user_id == user.id)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    session.title = body.title[:80]
    await db.commit()


@router.delete("/sessions/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(
    session_id: str,
    user: User = Depends(rate_limit),
    db: AsyncSession = Depends(get_db),
):
    deleted = await service.delete_session(db, user, session_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
