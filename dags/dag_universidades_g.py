from datetime import timedelta

default_args = {
    "retries": 5,  # Try 5 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}