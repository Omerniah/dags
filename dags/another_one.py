"""
Data Quality Check Pipeline using TaskFlow API
===============================================
Runs parallel data quality checks and aggregates results.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    dag_id="data_quality_checks",
    description="Parallel data quality validation pipeline",
    schedule="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "validation", "cicd"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
)
def data_quality_checks():
    """Run multiple data quality checks in parallel and report results."""

    @task
    def check_row_count() -> dict:
        """Verify minimum row count threshold."""
        # Simulated check
        actual_count = 1523
        threshold = 1000
        passed = actual_count >= threshold
        print(f"Row count: {actual_count} (threshold: {threshold})")
        return {"check": "row_count", "passed": passed, "details": f"{actual_count} rows"}

    @task
    def check_null_values() -> dict:
        """Check for null values in critical columns."""
        # Simulated check
        null_percentage = 2.3
        max_allowed = 5.0
        passed = null_percentage <= max_allowed
        print(f"Null percentage: {null_percentage}% (max allowed: {max_allowed}%)")
        return {"check": "null_values", "passed": passed, "details": f"{null_percentage}% nulls"}

    @task
    def check_duplicates() -> dict:
        """Detect duplicate records."""
        # Simulated check
        duplicate_count = 0
        passed = duplicate_count == 0
        print(f"Duplicates found: {duplicate_count}")
        return {"check": "duplicates", "passed": passed, "details": f"{duplicate_count} duplicates"}

    @task
    def check_freshness() -> dict:
        """Verify data is recent enough."""
        # Simulated check
        hours_old = 4
        max_hours = 24
        passed = hours_old <= max_hours
        print(f"Data age: {hours_old} hours (max: {max_hours} hours)")
        return {"check": "freshness", "passed": passed, "details": f"{hours_old}h old"}

    @task
    def aggregate_results(results: list[dict]) -> dict:
        """Combine all check results and determine overall status."""
        total = len(results)
        passed = sum(1 for r in results if r["passed"])
        failed = total - passed

        print("\n=== Data Quality Report ===")
        for result in results:
            status = "✓" if result["passed"] else "✗"
            print(f"  {status} {result['check']}: {result['details']}")

        overall_passed = failed == 0
        print(f"\nOverall: {passed}/{total} checks passed")

        if not overall_passed:
            raise ValueError(f"Data quality failed: {failed} check(s) did not pass")

        return {"total": total, "passed": passed, "failed": failed, "status": "SUCCESS"}

    # Run checks in parallel, then aggregate
    results = [
        check_row_count(),
        check_null_values(),
        check_duplicates(),
        check_freshness(),
    ]
    aggregate_results(results)


# Instantiate the DAG
data_quality_checks()
