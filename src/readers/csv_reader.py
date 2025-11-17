import pandas as pd
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

# Get the project root directory (2 levels up from this file)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def read_behavioral_csv(filepath=None):
    """Read behavioral data from CSV file"""
    if filepath is None:
        filepath = os.path.join(PROJECT_ROOT, 'data', 'behavioral.csv')

    try:
        if not os.path.exists(filepath):
            logger.warning(f"Behavioral CSV not found at {filepath}")
            return pd.DataFrame()

        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Convert Y/N to boolean
        bool_columns = ['breakfast_skipped', 'lunch_skipped', 'exercise']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].map({'Y': True, 'N': False, 'y': True, 'n': False})

        logger.debug(f"Read {len(df)} behavioral records")
        return df

    except Exception as e:
        logger.error(f"Error reading behavioral CSV: {e}")
        return pd.DataFrame()


def read_cognitive_csv(filepath=None):
    """Read cognitive test data from CSV file"""
    if filepath is None:
        filepath = os.path.join(PROJECT_ROOT, 'data', 'cognitive.csv')

    try:
        if not os.path.exists(filepath):
            logger.warning(f"Cognitive CSV not found at {filepath}")
            return pd.DataFrame()

        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        logger.debug(f"Read {len(df)} cognitive records")
        return df

    except Exception as e:
        logger.error(f"Error reading cognitive CSV: {e}")
        return pd.DataFrame()


def merge_user_data(behavioral_df, cognitive_df):
    """Merge behavioral and cognitive data on timestamp"""
    if behavioral_df.empty or cognitive_df.empty:
        return pd.DataFrame()

    merged = pd.merge(behavioral_df, cognitive_df, on='timestamp', how='inner')
    logger.debug(f"Merged {len(merged)} complete records")
    return merged


def read_all_user_data():
    """Read and merge all user data from CSVs"""
    behavioral = read_behavioral_csv()
    cognitive = read_cognitive_csv()
    merged = merge_user_data(behavioral, cognitive)

    if not merged.empty:
        return merged.to_dict('records')
    return []