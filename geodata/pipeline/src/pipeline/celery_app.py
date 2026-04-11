from celery import Celery
from celery.schedules import crontab

from pipeline.config import settings

app = Celery(
    "geodata",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["pipeline.tasks"],
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="America/Los_Angeles",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Beat schedule — data refresh cadence
    beat_schedule={
        # CDPH refreshes on the 11th business day of each month
        "ingest-cdph-monthly": {
            "task": "pipeline.tasks.ingest_cdph",
            "schedule": crontab(day_of_month="15", hour="2", minute="0"),
        },
        "ingest-crosswalk-monthly": {
            "task": "pipeline.tasks.ingest_crosswalk",
            "schedule": crontab(day_of_month="15", hour="3", minute="0"),
        },
        # Regenerate tiles nightly
        "generate-tiles-nightly": {
            "task": "pipeline.tasks.generate_all_tiles",
            "schedule": crontab(hour="4", minute="0"),
        },
    },
)
