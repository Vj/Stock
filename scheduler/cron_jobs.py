"""Cron-friendly entrypoint."""
from scheduler.daily_pipeline import run_pipeline


if __name__ == "__main__":
    run_pipeline()
