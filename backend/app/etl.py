"""ETL pipeline: fetch data from the autochecker API and load it into the database."""

import httpx
from datetime import datetime
from typing import Optional

from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func
from sqlalchemy import cast, JSON

from app.settings import settings
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _get_client() -> httpx.AsyncClient:
    """Create an HTTPX client with basic auth for the autochecker API."""
    return httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password),
        base_url=settings.autochecker_api_url,
        timeout=30.0,
    )


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------

async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with _get_client() as client:
        resp = await client.get("/api/items")
        resp.raise_for_status()
        return resp.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination using since."""
    all_logs = []
    limit = 500
    current_since = since

    async with _get_client() as client:
        while True:
            params = {"limit": limit}
            if current_since:
                params["since"] = current_since.isoformat().replace("+00:00", "Z")

            resp = await client.get("/api/logs", params=params)
            resp.raise_for_status()
            data = resp.json()

            logs = data.get("logs", [])
            if not logs:
                break

            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Update since to the submitted_at of the last log
            last_log = logs[-1]
            last_submitted = last_log.get("submitted_at")
            if not last_submitted:
                break
            # Convert to datetime for next iteration
            current_since = datetime.fromisoformat(last_submitted.replace("Z", "+00:00"))

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------

async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database. Return number of new items."""
    created_count = 0

    # Process labs first (type="lab")
    labs = [it for it in items if it["type"] == "lab"]
    lab_map = {}  # short lab id -> ItemRecord object

    for lab in labs:
        lab_id = lab["lab"]  # e.g. "lab-01"
        # Check if lab already exists (by attributes.lab)
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.attributes[("lab",)] == lab_id  # type: ignore
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if not existing:
            new_lab = ItemRecord(
                type="lab",
                title=lab["title"],
                attributes={"lab": lab_id},
                parent_id=None,
            )
            session.add(new_lab)
            await session.flush()
            lab_map[lab_id] = new_lab
            created_count += 1
        else:
            lab_map[lab_id] = existing

    # Process tasks (type="task")
    tasks = [it for it in items if it["type"] == "task"]
    for task in tasks:
        parent = lab_map.get(task["lab"])
        if not parent:
            continue

        task_id = task["task"]  # e.g. "setup"
        # Check if task already exists (by parent_id and attributes.task)
        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == parent.id,
            ItemRecord.attributes[("task",)] == task_id  # type: ignore
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if not existing:
            new_task = ItemRecord(
                type="task",
                title=task["title"],
                attributes={"lab": task["lab"], "task": task_id},
                parent_id=parent.id,
            )
            session.add(new_task)
            created_count += 1

    await session.commit()
    return created_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database. Return number of new interactions."""
    new_count = 0

    for log in logs:
        # Skip if this log already exists (idempotency)
        stmt = select(InteractionLog).where(InteractionLog.external_id == str(log["id"]))
        result = await session.execute(stmt)
        if result.scalar_one_or_none():
            continue

        # Find or create learner
        learner_stmt = select(Learner).where(Learner.external_id == log["student_id"])
        result = await session.execute(learner_stmt)
        learner = result.scalar_one_or_none()
        if not learner:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", ""),
            )
            session.add(learner)
            await session.flush()

        # Find the corresponding task item using lab and task fields in attributes
        item_stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.attributes[("lab",)] == log["lab"],  # type: ignore
            ItemRecord.attributes[("task",)] == log["task"],  # type: ignore
        )
        result = await session.execute(item_stmt)
        item = result.scalar_one_or_none()
        if not item:
            # No matching item in DB — skip this log
            continue

        # Create interaction log
        submitted = log.get("submitted_at")
        created_at = None
        if submitted:
            try:
                created_at = datetime.fromisoformat(submitted.replace("Z", "+00:00"))
            except Exception:
                created_at = None

        interaction = InteractionLog(
            external_id=str(log["id"]),
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at or datetime.utcnow(),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    # Step 1: fetch items catalog from API
    items_catalog = await fetch_items()

    # Step 2: load items into DB
    await load_items(items_catalog, session)

    # Step 3: determine last sync timestamp
    last_log_stmt = select(func.max(InteractionLog.created_at))
    result = await session.execute(last_log_stmt)
    last_created = result.scalar()

    # Step 4: fetch logs since that timestamp
    logs = await fetch_logs(since=last_created)

    # Step 5: load new logs
    new_records = await load_logs(logs, items_catalog, session)

    # Step 6: count total logs in DB
    total_stmt = select(func.count()).select_from(InteractionLog)
    result = await session.execute(total_stmt)
    total_records = result.scalar()

    return {"new_records": new_records, "total_records": total_records or 0}