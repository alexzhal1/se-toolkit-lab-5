"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, distinct, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lab_title_pattern(lab: str) -> str:
    """Convert 'lab-04' → 'Lab 04' for a LIKE/contains match."""
    _, number = lab.split("-", 1)
    return f"Lab {number}"


async def _get_lab_id(session: AsyncSession, lab: str) -> int:
    pattern = _lab_title_pattern(lab)
    stmt = select(ItemRecord.id).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(pattern),
    )
    result = await session.execute(stmt)
    return result.scalar_one()


async def _get_task_ids(session: AsyncSession, lab_id: int) -> list[int]:
    stmt = select(ItemRecord.id).where(ItemRecord.parent_id == lab_id)
    result = await session.execute(stmt)
    return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    lab_id = await _get_lab_id(session, lab)
    task_ids = await _get_task_ids(session, lab_id)

    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")

    stmt = (
        select(bucket_expr, func.count().label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None),
        )
        .group_by(bucket_expr)
    )

    result = await session.execute(stmt)
    counts = {row.bucket: row.count for row in result.all()}

    all_buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [{"bucket": b, "count": counts.get(b, 0)} for b in all_buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_id = await _get_lab_id(session, lab)

    stmt = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(
            ItemRecord.parent_id == lab_id,
            InteractionLog.score.isnot(None),
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    result = await session.execute(stmt)
    return [
        {"task": row.task, "avg_score": row.avg_score, "attempts": row.attempts}
        for row in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    lab_id = await _get_lab_id(session, lab)
    task_ids = await _get_task_ids(session, lab_id)

    date_col = func.date(InteractionLog.created_at).label("date")

    stmt = (
        select(date_col, func.count().label("submissions"))
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(date_col)
        .order_by(date_col)
    )

    result = await session.execute(stmt)
    return [
        {"date": row.date, "submissions": row.submissions}
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    lab_id = await _get_lab_id(session, lab)
    task_ids = await _get_task_ids(session, lab_id)

    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None),
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.execute(stmt)
    return [
        {"group": row.group, "avg_score": row.avg_score, "students": row.students}
        for row in result.all()
    ]