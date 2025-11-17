"""
Data Source Registry - Extensible system for adding new data sources

To add a new data source:
1. Create a reader function that returns a list of records
2. Register it in DATA_SOURCES with table_name, cleaner, and loader functions
3. Add validation rules in validators/validate.py if needed
"""

from typing import Callable, List, Dict, Any, Optional


class DataSource:
    """Represents a data source configuration"""
    
    def __init__(
        self,
        name: str,
        reader: Callable[[], List[Dict[str, Any]]],
        table_name: str,
        cleaner: Callable[[Dict[str, Any]], Dict[str, Any]],
        loader: Callable[[List[Dict[str, Any]]], tuple],
        validator_table: Optional[str] = None
    ):
        self.name = name
        self.reader = reader
        self.table_name = table_name
        self.cleaner = cleaner
        self.loader = loader
        self.validator_table = validator_table or table_name


# Import readers, cleaners, and loaders
from readers.api_reader import fetch_all_external_data
from readers.csv_reader import read_all_user_data
from cleaners.clean import clean_external_factors, clean_user_tracking, prepare_for_insert
from loaders.load import upsert_external_factors, insert_user_tracking


def _prepare_external_cleaner(record: Dict[str, Any]) -> Dict[str, Any]:
    """Wrapper to clean and prepare external factors"""
    cleaned = clean_external_factors(record)
    return prepare_for_insert(cleaned, 'external_factors')


def _prepare_user_cleaner(record: Dict[str, Any]) -> Dict[str, Any]:
    """Wrapper to clean and prepare user tracking"""
    cleaned = clean_user_tracking(record)
    return prepare_for_insert(cleaned, 'user_tracking')


def _wrap_external_reader() -> List[Dict[str, Any]]:
    """Wrapper to convert single record to list for consistency"""
    data = fetch_all_external_data()
    return [data] if data else []


# Registry of all data sources
DATA_SOURCES: List[DataSource] = [
    DataSource(
        name='external_factors_api',
        reader=_wrap_external_reader,
        table_name='external_factors',
        cleaner=_prepare_external_cleaner,
        loader=upsert_external_factors,
        validator_table='external_factors'
    ),
    DataSource(
        name='user_tracking_csv',
        reader=read_all_user_data,
        table_name='user_tracking',
        cleaner=_prepare_user_cleaner,
        loader=insert_user_tracking,
        validator_table='user_tracking'
    ),
]


def get_data_source(name: str) -> Optional[DataSource]:
    """Get a data source by name"""
    for source in DATA_SOURCES:
        if source.name == name:
            return source
    return None


def list_data_sources() -> List[str]:
    """List all registered data source names"""
    return [source.name for source in DATA_SOURCES]

