import pandas as pd
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _read_csv(default_filename: str, filepath: str | None, label: str) -> pd.DataFrame:
    if filepath is None:
        filepath = os.path.join(PROJECT_ROOT, "data", default_filename)

    try:
        if not os.path.exists(filepath):
            logger.warning(f"{label} CSV not found at {filepath}")
            return pd.DataFrame()

        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        logger.debug(f"Read {len(df)} {label.lower()} records")
        return df

    except Exception as e:
        logger.error(f"Error reading {label.lower()} CSV: {e}")
        return pd.DataFrame()


def read_behavioral_csv(filepath=None):
    return _read_csv("behavioral.csv", filepath, "Behavioral")


def read_cognitive_csv(filepath=None):
    return _read_csv("cognitive.csv", filepath, "Cognitive")


def merge_user_data(behavioral_df, cognitive_df):
    if behavioral_df.empty or cognitive_df.empty:
        return pd.DataFrame()

    merged = pd.merge(behavioral_df, cognitive_df, on='timestamp', how='inner')
    logger.debug(f"Merged {len(merged)} complete records")
    return merged


def read_all_user_data():
    behavioral = read_behavioral_csv()
    cognitive = read_cognitive_csv()
    merged = merge_user_data(behavioral, cognitive)
    if not merged.empty:
        return merged.to_dict('records')
    return []