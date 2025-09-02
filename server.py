import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import timedelta
import pandas as pd
from aws_utils import invoke_lambda
from typing import List
from fastapi import HTTPException
from logging import getLogger
logger = getLogger(__name__)

load_dotenv()

# Initialize the scheduler
scheduler = AsyncIOScheduler()


def perform_action() -> None:
    """Scheduled task executed every 12 hours.

    Replace the body of this function with the actual logic you want to run.
    """
    print(f"[scheduler] Running 12-hour task at {datetime.now(timezone.utc).isoformat()}")
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=3))
    url = "https://platform.happyrobot.ai/api/v1/runs"
    headers = {
        "anyuthorization": f"Bearer {os.getenv('PLATFORM_API_KEY')}",
        "x-organization-id": os.getenv('DCL_ORG_ID'),
    }
    start_utc = datetime.now(timezone.utc) - timedelta(hours=12)
    params = {
        "limit": 100,
        "use_case_id": os.getenv('PAYMENT_STATUS_USE_CASE_ID'),
         "start": start_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
    }

    response = session.get(url, headers=headers, params=params, timeout=(3.05, 20))
    response.raise_for_status()
    runs = response.json()
    print(f"[scheduler] Found {len(runs)} runs")
    for run in runs:
        print(f"[scheduler] Run {run['id']} - {run['status']}")
    
    calls_data = response.json()["data"]
    df = pd.DataFrame(calls_data)
    total_calls_past_12_hours = len(df)
    total_calls_past_12_hours_failed = len(df[df['could_not_find_load_id'] == 'did_not_find_load'])
    total_calls_past_12_hours_failed_percentage = total_calls_past_12_hours_failed / total_calls_past_12_hours
    print(f"[scheduler] Total calls past 12 hours: {total_calls_past_12_hours}")
    print(f"[scheduler] Total calls past 12 hours failed: {total_calls_past_12_hours_failed}")
    print(f"[scheduler] Total calls past 12 hours failed percentage: {total_calls_past_12_hours_failed_percentage}")

    if total_calls_past_12_hours_failed_percentage > 0.25:
        print("[scheduler] Total calls past 12 hours failed percentage is greater than 25%")
        print(f"[scheduler] Sending email to {os.getenv('EMAIL_TO')}")
        send_email( [os.getenv('EMAIL_TO')], "Payment Status Audit Happy Robot", f"Total calls past 12 hours failed percentage is greater than 25%. We are seeing a rate of {total_calls_past_12_hours_failed_percentage*100}% of calls failing to find a load id.")
    else:
        print("[scheduler] Total calls past 12 hours failed percentage is less than 25%")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan to start and stop the scheduler."""
    scheduler.add_job(
        perform_action,
        IntervalTrigger(hours=12),
        id="twelve_hour_job",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.start()
    try:
        yield
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)



def send_email(
    email_addresses: List[str] = [],
    subject: str = "",
    body: str = "",
) -> dict:
    try:

        payload = {
            "orgId": os.getenv("DCL_ORG_ID"),
            "from": os.getenv("SENDER_EMAIL"),
            "to": email_addresses,
            "subject": subject,
            "body": body,
        }
        invoke_lambda(
            region=os.getenv("AWS_REGION"),
            function_name=os.getenv("LAMBDA_FUNCTION_NAME"),
            payload=payload,
        )
        return {
            "success": True,
            "message": "Email sent successfully",
            "email_addresses": email_addresses,
        }
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error sending email: {str(e)}"
        )



app = FastAPI(lifespan=lifespan)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
