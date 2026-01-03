from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from notifications.service import list_notifications, mark_notification_read

router = APIRouter(prefix="/notifications", tags=["notifications"])


class NotificationReadRequest(BaseModel):
    user_id: int = Field(description="User ID")


@router.get("", status_code=status.HTTP_200_OK)
async def get_notifications(
    user_id: int = Query(..., description="User ID"),
    unread_only: bool = Query(False, description="Return only unread notifications"),
    limit: int = Query(50, ge=1, le=200, description="Max number of notifications"),
):
    return {
        "notifications": await list_notifications(
            user_id=user_id, unread_only=unread_only, limit=limit
        )
    }


@router.post("/{notification_id}/read", status_code=status.HTTP_200_OK)
async def read_notification(notification_id: int, body: NotificationReadRequest):
    ok = await mark_notification_read(user_id=body.user_id, notification_id=notification_id)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="알림을 찾을 수 없거나 이미 읽음 처리되었습니다.",
        )
    return {"ok": True}


