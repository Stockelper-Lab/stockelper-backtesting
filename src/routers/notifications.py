from __future__ import annotations

from fastapi import APIRouter, Query, status

router = APIRouter(prefix="/notifications", tags=["notifications"])


@router.get("", status_code=status.HTTP_200_OK)
async def list_notifications(
    user_id: int = Query(..., description="stockelper_web.users.id"),
    unread_only: bool = Query(False, description="읽지 않은 알림만 조회"),
    limit: int = Query(50, ge=1, le=200),
):
    """알림 목록 (현재는 스텁).

    NOTE: 현 시점 요구사항은 백테스트 결과 적재가 핵심이라,
    notifications 스키마/거버넌스 확정 전까지는 최소 동작만 제공합니다.
    """

    return {"user_id": user_id, "unread_only": unread_only, "items": [], "limit": limit}


@router.post("/{notification_id}/read", status_code=status.HTTP_200_OK)
async def mark_read(
    notification_id: str,
    user_id: int = Query(..., description="stockelper_web.users.id"),
):
    """알림 읽음 처리 (현재는 스텁)."""

    return {"ok": True, "notification_id": notification_id, "user_id": user_id}

