"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, cast, Date, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_from_param(lab: str) -> str:
  # 'lab-04' -> 'Lab 04'
  lab_num = lab.replace("lab-", "")
  return f"Lab {lab_num}"


@router.get("/scores")
async def get_scores(
  lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
  session: AsyncSession = Depends(get_session),
):
  """Score distribution histogram for a given lab.

  Returns 4 exact buckets: '0-25', '26-50', '51-75', '76-100'.
  """
  lab_title_pattern = _lab_title_from_param(lab)

  lab_row = (await session.exec(
    select(ItemRecord).where(
      (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
  )).scalars().first()
  if not lab_row:
    # return empty buckets
    return [
      {"bucket": "0-25", "count": 0},
      {"bucket": "26-50", "count": 0},
      {"bucket": "51-75", "count": 0},
      {"bucket": "76-100", "count": 0},
    ]

  tasks = (await session.exec(
    select(ItemRecord).where((ItemRecord.type == "task") & (ItemRecord.parent_id == lab_row.id))
  )).scalars().all()
  task_ids = [t.id for t in tasks]

  if not task_ids:
    return [
      {"bucket": "0-25", "count": 0},
      {"bucket": "26-50", "count": 0},
      {"bucket": "51-75", "count": 0},
      {"bucket": "76-100", "count": 0},
    ]

  bucket_case = case(
    (InteractionLog.score <= 25, "0-25"),
    (InteractionLog.score <= 50, "26-50"),
    (InteractionLog.score <= 75, "51-75"),
    else_="76-100",
  )

  rows = (await session.exec(
    select(bucket_case.label("bucket"), func.count().label("count")).where(
      (InteractionLog.item_id.in_(task_ids)) & (InteractionLog.score.isnot(None))
    ).group_by(bucket_case)
  )).all()

  counts = {r.bucket: r.count for r in rows}

  return [
    {"bucket": "0-25", "count": counts.get("0-25", 0)},
    {"bucket": "26-50", "count": counts.get("26-50", 0)},
    {"bucket": "51-75", "count": counts.get("51-75", 0)},
    {"bucket": "76-100", "count": counts.get("76-100", 0)},
  ]


@router.get("/pass-rates")
async def get_pass_rates(
  lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
  session: AsyncSession = Depends(get_session),
):
  """Per-task pass rates for a given lab.
  Returns per-task avg_score (1 decimal) and attempts count, ordered by task title.
  """
  lab_title_pattern = _lab_title_from_param(lab)

  lab_row = (await session.exec(
    select(ItemRecord).where(
      (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
  )).scalars().first()
  if not lab_row:
    return []

  tasks = (await session.exec(
    select(ItemRecord).where((ItemRecord.type == "task") & (ItemRecord.parent_id == lab_row.id)).order_by(ItemRecord.title)
  )).scalars().all()

  # Use a single query joining ItemRecord and InteractionLog to avoid model/row shape issues
  rows = (await session.exec(
    select(
      ItemRecord.title.label("task"),
      func.avg(InteractionLog.score).label("avg_score"),
      func.count().label("attempts"),
    ).join(InteractionLog, InteractionLog.item_id == ItemRecord.id).where(
      (ItemRecord.parent_id == lab_row.id) & (InteractionLog.score.isnot(None))
    ).group_by(ItemRecord.title).order_by(ItemRecord.title)
  )).all()

  out = []
  for r in rows:
    avg_score = round(r.avg_score, 1) if r.avg_score is not None else 0.0
    attempts = int(r.attempts) if r.attempts is not None else 0
    out.append({"task": r.task, "avg_score": avg_score, "attempts": attempts})

  return out


@router.get("/timeline")
async def get_timeline(
  lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
  session: AsyncSession = Depends(get_session),
):
  """Submissions per day for a given lab.
  Groups by date (cast created_at to Date) and counts submissions.
  """
  lab_title_pattern = _lab_title_from_param(lab)

  lab_row = (await session.exec(
    select(ItemRecord).where(
      (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
  )).scalars().first()
  if not lab_row:
    return []

  tasks = (await session.exec(
    select(ItemRecord).where((ItemRecord.type == "task") & (ItemRecord.parent_id == lab_row.id))
  )).scalars().all()
  task_ids = [t.id for t in tasks]
  if not task_ids:
    return []

  # Use func.date to extract the date part; works reliably across SQLite
  date_expr = func.date(InteractionLog.created_at)

  rows = (await session.exec(
    select(date_expr.label("date"), func.count().label("submissions")).where(
      InteractionLog.item_id.in_(task_ids)
    ).group_by(date_expr).order_by(date_expr)
  )).all()

  return [{"date": str(r.date), "submissions": r.submissions} for r in rows]


@router.get("/groups")
async def get_groups(
  lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
  session: AsyncSession = Depends(get_session),
):
  """Per-group performance for a given lab.
  Joins interactions with learners and computes avg score (1 decimal) and distinct student count per group.
  """
  lab_title_pattern = _lab_title_from_param(lab)

  lab_row = (await session.exec(
    select(ItemRecord).where(
      (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
  )).scalars().first()
  if not lab_row:
    return []

  tasks = (await session.exec(
    select(ItemRecord).where((ItemRecord.type == "task") & (ItemRecord.parent_id == lab_row.id))
  )).scalars().all()
  task_ids = [t.id for t in tasks]
  if not task_ids:
    return []

  rows = (await session.exec(
    select(
      Learner.student_group.label("group"),
      func.avg(InteractionLog.score).label("avg_score"),
      func.count(func.distinct(Learner.id)).label("students"),
    ).join(InteractionLog, InteractionLog.learner_id == Learner.id).where(
      (InteractionLog.item_id.in_(task_ids)) & (InteractionLog.score.isnot(None))
    ).group_by(Learner.student_group).order_by(Learner.student_group)
  )).all()

  return [
    {
      "group": r.group,
      "avg_score": round(r.avg_score, 1) if r.avg_score is not None else 0.0,
      "students": int(r.students),
    }
    for r in rows
  ]
