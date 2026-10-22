from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import get_session
from app.core.security import verify_api_key
from app.schemas.payment import PaymentCreate, PaymentCreatedResponse, PaymentResponse
from app.services.payment import (
    create_payment,
    get_payment_by_id,
    payment_to_created_response,
    payment_to_response,
)

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentCreatedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(verify_api_key)],
)
async def create_payment_endpoint(
    payload: PaymentCreate,
    response: Response,
    session: AsyncSession = Depends(get_session),
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
) -> PaymentCreatedResponse:
    if not idempotency_key.strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Idempotency-Key must not be empty",
        )

    payment, created = await create_payment(session, payload, idempotency_key.strip())
    if not created:
        response.status_code = status.HTTP_200_OK
    return payment_to_created_response(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_payment_endpoint(
    payment_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:
    payment = await get_payment_by_id(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return payment_to_response(payment)
