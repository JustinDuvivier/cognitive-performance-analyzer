import logging
from pathlib import Path

import pandas as pd

from config.config import PROJECT_ROOT

logger = logging.getLogger(__name__)


def _read_csv(default_filename: str, filepath: str | None, label: str) -> pd.DataFrame:
    if filepath is None:
        filepath = PROJECT_ROOT / "data" / default_filename

    try:
        if not Path(filepath).exists():
            logger.warning(f"{label} CSV not found at {filepath}")
            return pd.DataFrame()

        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        if 'person' not in df.columns:
            logger.warning(f"{label} CSV missing 'person' column, defaulting to 'Unknown'")
            df['person'] = 'Unknown'

        logger.debug(f"Read {len(df)} {label.lower()} records")
        return df

    except Exception as e:
        logger.error(f"Error reading {label.lower()} CSV: {e}")
        return pd.DataFrame()


def read_behavioral_csv(filepath: str | None = None) -> pd.DataFrame:
    return _read_csv("behavioral.csv", filepath, "Behavioral")


def read_cognitive_csv(filepath: str | None = None) -> pd.DataFrame:
    return _read_csv("cognitive.csv", filepath, "Cognitive")


def read_external_csv(filepath: str | None = None) -> pd.DataFrame:
    return _read_csv("external.csv", filepath, "External")


def merge_user_data(behavioral_df: pd.DataFrame, cognitive_df: pd.DataFrame) -> pd.DataFrame:
    if behavioral_df.empty or cognitive_df.empty:
        return pd.DataFrame()

    merged = pd.merge(behavioral_df, cognitive_df, on=['person', 'timestamp'], how='inner')
    logger.debug(f"Merged {len(merged)} complete records")
    return merged


def read_all_user_data() -> list[dict]:
    behavioral = read_behavioral_csv()
    cognitive = read_cognitive_csv()
    merged = merge_user_data(behavioral, cognitive)
    if not merged.empty:
        return merged.to_dict('records')
    return []


def read_all_external_data() -> list[dict]:
    external = read_external_csv()
    if not external.empty:
        return external.to_dict('records')
    return []
