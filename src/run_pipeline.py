import sys
from datetime import datetime

from readers.csv_reader import read_all_user_data, read_all_external_data
from validators.validate import validate_batch
from cleaners.clean import clean_measurement_external, clean_measurement_user, prepare_for_insert
from loaders.load import upsert_measurement_external, upsert_measurement_user, check_table_counts
from loggers.logger import (
    setup_logging,
    get_logger,
    log_rejected_records,
    log_pipeline_start,
    log_pipeline_end,
    log_validation_warning,
    log_db_warning,
    format_rejection,
)

setup_logging()
logger = get_logger(__name__)


def format_invalid_for_rejects(invalid: dict) -> dict:
    return format_rejection(
        table=invalid.get("table", "unknown"),
        record=invalid.get("record", {}),
        errors=invalid.get("errors", ["Unknown error"]),
    )


def validate_clean_and_load(
    raw_records: list[dict],
    validator_name: str,
    stats: dict,
    rejected: list[dict],
    cleaner,
    loader,
) -> tuple[dict, list[dict]]:
    valid, invalid = validate_batch(raw_records, validator_name)
    stats["validated"] = len(valid)
    stats["rejected"] = len(invalid)

    if invalid:
        rejected.extend(format_invalid_for_rejects(r) for r in invalid)
        log_validation_warning(validator_name, len(invalid))

    if not valid:
        return stats, rejected

    cleaned_prepared = []
    for rec in valid:
        cleaned = cleaner(rec)
        prepared = prepare_for_insert(cleaned)
        cleaned_prepared.append(prepared)

    inserted, db_rejected = loader(cleaned_prepared)
    stats["loaded"] = inserted
    stats["db_rejected"] = len(db_rejected)

    if db_rejected:
        rejected.extend(db_rejected)
        log_db_warning(validator_name, len(db_rejected))

    return stats, rejected


def run_measurement_external_flow() -> tuple[dict, list]:
    stats = {
        "name": "measurements_external",
        "read": 0,
        "validated": 0,
        "rejected": 0,
        "loaded": 0,
        "db_rejected": 0,
    }
    rejected = []

    logger.info("Reading external factors from CSV...")
    raw_records = read_all_external_data()

    if not raw_records:
        logger.warning("No external data found in CSV")
        return stats, rejected

    stats["read"] = len(raw_records)
    logger.info(f"Read {stats['read']} external records from CSV")

    return validate_clean_and_load(
        raw_records,
        "measurements_external",
        stats,
        rejected,
        clean_measurement_external,
        upsert_measurement_external,
    )


def run_measurement_user_flow() -> tuple[dict, list]:
    stats = {
        "name": "measurements_user",
        "read": 0,
        "validated": 0,
        "rejected": 0,
        "loaded": 0,
        "db_rejected": 0,
    }
    rejected = []

    logger.info("Reading user tracking data from CSVs...")
    raw_records = read_all_user_data()
    stats["read"] = len(raw_records)

    if not raw_records:
        logger.warning("No user tracking data found in CSVs")
        return stats, rejected

    logger.info(f"Read {stats['read']} user tracking records")

    return validate_clean_and_load(
        raw_records,
        "measurements_user",
        stats,
        rejected,
        clean_measurement_user,
        upsert_measurement_user,
    )


def run_pipeline() -> dict:
    start_time = log_pipeline_start("COGNITIVE PERFORMANCE PIPELINE")

    external_stats, external_rejected = run_measurement_external_flow()
    user_stats, user_rejected = run_measurement_user_flow()

    all_stats = [external_stats, user_stats]
    all_rejected = external_rejected + user_rejected

    if all_rejected:
        logger.info("Logging rejected records...")
        logged = log_rejected_records(all_rejected)
        logger.info(f"Logged {logged} rejected records to rejected_records")

    total_read = sum(s["read"] for s in all_stats)
    total_validated = sum(s["validated"] for s in all_stats)
    total_loaded = sum(s["loaded"] for s in all_stats)
    total_rejected = sum(s["rejected"] + s.get("db_rejected", 0) for s in all_stats)

    counts = check_table_counts()

    log_pipeline_end("COGNITIVE PERFORMANCE PIPELINE", start_time, {
        "total_read": total_read,
        "total_validated": total_validated,
        "total_loaded": total_loaded,
        "total_rejected": total_rejected,
        "source_stats": all_stats,
        "db_counts": counts,
    })

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
        "success": True,
        "duration_seconds": duration,
        "sources_processed": 2,
        "total_read": total_read,
        "total_validated": total_validated,
        "total_loaded": total_loaded,
        "total_rejected": total_rejected,
        "source_stats": all_stats,
        "start_time": start_time,
        "end_time": end_time,
    }


if __name__ == "__main__":
    try:
        result = run_pipeline()
        sys.exit(0 if result.get("success", False) else 1)
    except Exception as exc:
        logger.error(f"Fatal error: {exc}")
        sys.exit(1)
