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
        # CDPH facility locations — monthly
        "ingest-cdph-monthly": {
            "task": "pipeline.tasks.ingest_cdph",
            "schedule": crontab(day_of_month="15", hour="2", minute="0"),
        },
        "ingest-crosswalk-monthly": {
            "task": "pipeline.tasks.ingest_crosswalk",
            "schedule": crontab(day_of_month="15", hour="3", minute="0"),
        },
        # CMS Nursing Home Health Deficiencies — monthly, 1st of month
        "ingest-cms-nh-compare-monthly": {
            "task": "pipeline.tasks.ingest_cms_nh_compare",
            "schedule": crontab(day_of_month="1", hour="5", minute="0"),
        },
        # CDPH State Enforcement Actions — annual, July 15
        "ingest-cdph-sea-annual": {
            "task": "pipeline.tasks.ingest_cdph_sea",
            "schedule": crontab(month_of_year="7", day_of_month="15", hour="6", minute="0"),
        },
        # Regenerate tiles nightly
        "generate-tiles-nightly": {
            "task": "pipeline.tasks.generate_all_tiles",
            "schedule": crontab(hour="4", minute="0"),
        },
    },
)
