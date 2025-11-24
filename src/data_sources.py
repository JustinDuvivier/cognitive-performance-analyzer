from typing import Callable, List, Dict, Any, Optional
import importlib
import os

import yaml


class DataSource:
    def __init__(
        self,
        name: str,
        reader: Callable[[], List[Dict[str, Any]]],
        table_name: str,
        cleaner: Callable[[Dict[str, Any]], Dict[str, Any]],
        loader: Callable[[List[Dict[str, Any]]], tuple],
        validator_table: Optional[str] = None,
    ):
        self.name = name
        self.reader = reader
        self.table_name = table_name
        self.cleaner = cleaner
        self.loader = loader
        self.validator_table = validator_table or table_name


def _resolve_callable(path: str) -> Callable:
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def _load_data_source_configs() -> List[Dict[str, Any]]:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "data_sources.yaml")

    if not os.path.exists(config_path):
        return []

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    return config.get("data_sources", [])


def _build_data_source(config: Dict[str, Any]) -> DataSource:
    name = config["name"]
    table_name = config["table_name"]
    validator_table = config.get("validator_table", table_name)

    reader_callable = _resolve_callable(config["reader"])
    cleaner_callable = _resolve_callable(config["cleaner"])
    prepare_for_insert_callable = _resolve_callable(config["prepare_for_insert"])
    loader_callable = _resolve_callable(config["loader"])

    single_record = bool(config.get("single_record", False))

    def wrapped_reader() -> List[Dict[str, Any]]:
        data = reader_callable()
        if single_record:
            return [data] if data else []
        return data or []

    def wrapped_cleaner(record: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = cleaner_callable(record)
        return prepare_for_insert_callable(cleaned)

    return DataSource(
        name=name,
        reader=wrapped_reader,
        table_name=table_name,
        cleaner=wrapped_cleaner,
        loader=loader_callable,
        validator_table=validator_table,
    )


def _load_data_sources() -> List[DataSource]:
    raw_configs = _load_data_source_configs()
    return [_build_data_source(cfg) for cfg in raw_configs]


DATA_SOURCES: List[DataSource] = _load_data_sources()

