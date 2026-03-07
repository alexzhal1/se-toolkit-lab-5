"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": 500}
            if current_since:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Use the submitted_at of the last log as the new "since" value
            if logs:
                current_since = datetime.fromisoformat(logs[-1]["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    new_items_count = 0
    labs_by_short_id: dict[str, ItemRecord] = {}

    # Process labs first
    labs = [item for item in items if item.get("type") == "lab"]
    for lab_item in labs:
        lab_short_id = lab_item["lab"]
        lab_title = lab_item["title"]

        # Check if lab already exists
        existing = await session.exec(
            select(ItemRecord).where(
                (ItemRecord.type == "lab") & (ItemRecord.title == lab_title)
            )
        )
        existing_record = existing.first()

        if not existing_record:
            # Create new lab
            new_lab = ItemRecord(type="lab", title=lab_title)
            session.add(new_lab)
            await session.flush()  # Flush to get the ID
            labs_by_short_id[lab_short_id] = new_lab
            new_items_count += 1
        else:
            labs_by_short_id[lab_short_id] = existing_record

    # Process tasks
    tasks = [item for item in items if item.get("type") == "task"]
    for task_item in tasks:
        lab_short_id = task_item["lab"]
        task_title = task_item["title"]

        # Find parent lab
        parent_lab = labs_by_short_id.get(lab_short_id)
        if not parent_lab:
            continue  # Skip if parent lab not found

        # Check if task already exists
        existing = await session.exec(
            select(ItemRecord).where(
                (ItemRecord.type == "task")
                & (ItemRecord.title == task_title)
                & (ItemRecord.parent_id == parent_lab.id)
            )
        )
        existing_record = existing.first()

        if not existing_record:
            # Create new task
            new_task = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(new_task)
            new_items_count += 1

    await session.commit()
    return new_items_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    new_interactions_count = 0

    # Build lookup from (lab, task) to item title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_id = item["lab"]
        task_id = item.get("task")
        title = item["title"]
        item_title_lookup[(lab_id, task_id)] = title

    for log in logs:
        # 1. Find or create a Learner by external_id
        student_id = log["student_id"]
        existing_learner = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = existing_learner.first()

        if not learner:
            learner = Learner(external_id=student_id, student_group=log.get("group", ""))
            session.add(learner)
            await session.flush()  # Get the ID

        # 2. Find the matching item in the database
        lab_short_id = log["lab"]
        task_short_id = log.get("task")

        item_title = item_title_lookup.get((lab_short_id, task_short_id))
        if not item_title:
            continue  # Skip if no matching item found

        existing_item = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = existing_item.first()
        if not item:
            continue  # Skip if item not in database

        # 3. Check if InteractionLog with this external_id already exists
        log_external_id = log["id"]
        existing_interaction = await session.exec(
            select(InteractionLog).where(
                InteractionLog.external_id == log_external_id
            )
        )
        if existing_interaction.first():
            continue  # Skip if already exists (idempotent)

        # 4. Create InteractionLog
        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(interaction)
        new_interactions_count += 1

    await session.commit()
    return new_interactions_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    # Step 1: Fetch and load items
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    # Step 2: Determine the last synced timestamp
    last_interaction = await session.exec(
        select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    )
    last_log = last_interaction.first()
    since = last_log.created_at if last_log else None

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since)
    new_records = await load_logs(logs, items_catalog, session)

    # Get total count
    total_result = await session.exec(select(InteractionLog))
    total_records = len(total_result.all())

    return {"new_records": new_records, "total_records": total_records}
