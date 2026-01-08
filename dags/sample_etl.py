"""
Sample DAG using TaskFlow API for CI/CD Testing
================================================
A simple ETL pipeline that extracts, transforms, and loads sample data.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    dag_id="sample_etl_pipeline",
    description="Sample ETL pipeline for CI/CD testing",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sample", "etl", "cicd"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def sample_etl_pipeline():
    """A simple ETL pipeline demonstrating TaskFlow API."""

    @task
    def extract() -> dict:
        """Extract sample data (simulates API call or DB query)."""
        raw_data = {
            "users": [
                {"id": 1, "name": "Alice", "score": 85},
                {"id": 2, "name": "Bob", "score": 92},
                {"id": 3, "name": "Charlie", "score": 78},
            ]
        }
        print(f"Extracted {len(raw_data['users'])} records")
        return raw_data

    @task
    def transform(raw_data: dict) -> dict:
        """Transform data: add grade based on score."""
        def get_grade(score: int) -> str:
            if score >= 90:
                return "A"
            elif score >= 80:
                return "B"
            elif score >= 70:
                return "C"
            return "D"

        transformed = []
        for user in raw_data["users"]:
            transformed.append({
                **user,
                "grade": get_grade(user["score"]),
            })

        print(f"Transformed {len(transformed)} records")
        return {"users": transformed}

    @task
    def load(transformed_data: dict) -> None:
        """Load data (simulates writing to DB or file)."""
        for user in transformed_data["users"]:
            print(f"Loaded: {user['name']} - Score: {user['score']} - Grade: {user['grade']}")
        print(f"Successfully loaded {len(transformed_data['users'])} records")

    # Define task dependencies using TaskFlow
    raw = extract()
    transformed = transform(raw)
    load(transformed)


# Instantiate the DAG
sample_etl_pipeline()
