from __future__ import annotations

import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple, Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from readers.api_reader import fetch_all_external_data
from readers.csv_reader import read_all_user_data
from validators.validate import validate_batch
from cleaners.clean import clean_external_factors, clean_user_tracking, prepare_for_insert
from loaders.load import upsert_external_factors, insert_user_tracking, log_rejected_records, check_table_counts
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _format_invalid_for_rejects(invalid: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "table": invalid.get("table", "unknown"),
        "record": invalid.get("record", {}),
        "error": "; ".join(invalid.get("errors", ["Unknown error"])),
    }


def _validate_clean_and_load(
    raw_records: List[Dict[str, Any]],
    table_name: str,
    stats: Dict[str, Any],
    rejected: List[Dict[str, Any]],
    cleaner: Callable[[Dict[str, Any]], Dict[str, Any]],
    loader: Callable[[List[Dict[str, Any]]], Tuple[int, List[Dict[str, Any]]]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Shared validate → clean → load flow for a batch of records."""
    valid, invalid = validate_batch(raw_records, table_name)
    stats["validated"] = len(valid)
    stats["rejected"] = len(invalid)

    if invalid:
        rejected.extend(_format_invalid_for_rejects(r) for r in invalid)
        logger.warning(f"{table_name}: {len(invalid)} records failed validation")

    if not valid:
        return stats, rejected

    cleaned_prepared: List[Dict[str, Any]] = []
    for rec in valid:
        cleaned = cleaner(rec)
        prepared = prepare_for_insert(cleaned)
        cleaned_prepared.append(prepared)

    inserted, db_rejected = loader(cleaned_prepared)
    stats["loaded"] = inserted
    stats["db_rejected"] = len(db_rejected)

    if db_rejected:
        rejected.extend(db_rejected)
        logger.warning(f"{table_name}: {len(db_rejected)} records failed to load")

    return stats, rejected


def _run_external_factors_flow() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Run the external_factors flow."""
    stats = {
        "name": "external_factors",
        "read": 0,
        "validated": 0,
        "rejected": 0,
        "loaded": 0,
        "db_rejected": 0,
    }
    rejected: List[Dict[str, Any]] = []

    logger.info("Reading external factors (weather + air quality)...")
    record = fetch_all_external_data()
    if record is None:
        logger.warning("No external data fetched")
        return stats, rejected

    raw_records = [record]
    stats["read"] = len(raw_records)
    logger.info(f"Read {stats['read']} external record")

    return _validate_clean_and_load(
        raw_records,
        "external_factors",
        stats,
        rejected,
        clean_external_factors,
        upsert_external_factors,
    )


def _run_user_tracking_flow() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    stats = {
        "name": "user_tracking",
        "read": 0,
        "validated": 0,
        "rejected": 0,
        "loaded": 0,
        "db_rejected": 0,
    }
    rejected: List[Dict[str, Any]] = []

    logger.info("Reading user tracking data from CSVs...")
    raw_records = read_all_user_data()
    stats["read"] = len(raw_records)

    if not raw_records:
        logger.warning("No user tracking data found in CSVs")
        return stats, rejected

    logger.info(f"Read {stats['read']} user tracking records")

    return _validate_clean_and_load(
        raw_records,
        "user_tracking",
        stats,
        rejected,
        clean_user_tracking,
        insert_user_tracking,
    )


def run_pipeline() -> Dict[str, Any]:
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("BRAIN FOG PIPELINE - Starting ingestion")
    logger.info("=" * 60)

    external_stats, external_rejected = _run_external_factors_flow()
    user_stats, user_rejected = _run_user_tracking_flow()

    all_stats = [external_stats, user_stats]
    all_rejected = external_rejected + user_rejected

    if all_rejected:
        logger.info("Logging rejected records...")
        logged = log_rejected_records(all_rejected)
        logger.info(f"Logged {logged} rejected records to stg_rejects")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    total_read = sum(s["read"] for s in all_stats)
    total_validated = sum(s["validated"] for s in all_stats)
    total_loaded = sum(s["loaded"] for s in all_stats)
    total_rejected = sum(s["rejected"] + s.get("db_rejected", 0) for s in all_stats)

    logger.info("\nPIPELINE SUMMARY")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info("\nTotal Records:")
    logger.info(f"  Read: {total_read}")
    logger.info(f"  Validated: {total_validated}")
    logger.info(f"  Loaded: {total_loaded}")
    logger.info(f"  Rejected: {total_rejected}")

    logger.info("\nBy Source:")
    for stats in all_stats:
        logger.info(f"  {stats['name']}:")
        logger.info(
            "    Read: {read}, Validated: {validated}, Loaded: {loaded}, Rejected: {rejected}".format(
                read=stats["read"],
                validated=stats["validated"],
                loaded=stats["loaded"],
                rejected=stats["rejected"] + stats.get("db_rejected", 0),
            )
        )

    counts = check_table_counts()
    if counts:
        logger.info("\nDatabase Counts:")
        for table, count in counts.items():
            logger.info(f"  {table}: {count} records")

    logger.info("=" * 60)
    logger.info("✅ PIPELINE COMPLETE")
    logger.info("=" * 60)

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
