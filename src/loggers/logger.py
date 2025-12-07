import json
import logging
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from config.config import DB_CONFIG

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=LOG_FORMAT)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def _get_db_connection() -> psycopg2.extensions.connection | None:
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logging.error(f"Failed to connect to database for logging: {e}")
        return None


def log_rejected_records(rejected_records: list[dict]) -> int:
    if not rejected_records:
        return 0

    conn = _get_db_connection()
    if not conn:
        return 0

    logged = 0
    logger = get_logger(__name__)
    cur = None

    try:
        cur = conn.cursor()

        values = [
            (
                reject.get('table', 'unknown'),
                json.dumps(reject.get('record', {}), default=str),
                reject.get('error', 'Unknown error')[:500],
            )
            for reject in rejected_records
        ]

        execute_values(
            cur,
            """INSERT INTO rejected_records (source_name, raw_payload, reason) VALUES %s""",
            values
        )

        logged = len(values)
        conn.commit()
        logger.debug(f"Logged {logged} rejected records to rejected_records")

    except Exception as e:
        logger.error(f"Failed to log rejected records: {e}")
        conn.rollback()
    finally:
        if cur:
            cur.close()
        conn.close()

    return logged


def log_pipeline_start(pipeline_name: str) -> datetime:
    logger = get_logger("pipeline")
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"{pipeline_name} - Starting")
    logger.info("=" * 60)
    return start_time


def log_pipeline_end(pipeline_name: str, start_time: datetime, stats: dict[str, Any]) -> None:
    logger = get_logger("pipeline")
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info("")
    logger.info("PIPELINE SUMMARY")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info("")
    logger.info("Total Records:")
    logger.info(f"  Read: {stats.get('total_read', 0)}")
    logger.info(f"  Validated: {stats.get('total_validated', 0)}")
    logger.info(f"  Loaded: {stats.get('total_loaded', 0)}")
    logger.info(f"  Rejected: {stats.get('total_rejected', 0)}")

    if 'source_stats' in stats:
        logger.info("")
        logger.info("By Source:")
        for source in stats['source_stats']:
            logger.info(f"  {source['name']}:")
            logger.info(
                f"    Read: {source['read']}, Validated: {source['validated']}, "
                f"Loaded: {source['loaded']}, Rejected: {source['rejected'] + source.get('db_rejected', 0)}"
            )

    if 'db_counts' in stats:
        logger.info("")
        logger.info("Database Counts:")
        for table, count in stats['db_counts'].items():
            logger.info(f"  {table}: {count}")

    logger.info("=" * 60)
    logger.info(f"✅ {pipeline_name} - COMPLETE")
    logger.info("=" * 60)


def log_source_start(_source_name: str, action: str) -> None:
    logger = get_logger("pipeline")
    logger.info(f"{action}...")


def log_source_result(_source_name: str, count: int, action: str) -> None:
    logger = get_logger("pipeline")
    logger.info(f"{action}: {count} records")


def log_validation_warning(source_name: str, count: int) -> None:
    logger = get_logger("pipeline")
    logger.warning(f"{source_name}: {count} records failed validation")


def log_db_warning(source_name: str, count: int) -> None:
    logger = get_logger("pipeline")
    logger.warning(f"{source_name}: {count} records failed to load")


def format_rejection(table: str, record: dict, errors: list[str]) -> dict:
    return {
        "table": table,
        "record": record,
        "error": "; ".join(errors) if errors else "Unknown error",
    }

