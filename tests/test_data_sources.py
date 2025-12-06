import os

import pytest

from data_sources import DataSource, _resolve_callable, DATA_SOURCES


class TestDataSource:
    def test_initialization(self):
        ds = DataSource(name="test", reader=os.getcwd, table_name="test_table", cleaner=os.getcwd, loader=lambda x: (0, []))
        assert ds.name == "test"
        assert ds.table_name == "test_table"
        assert callable(ds.reader)
        assert callable(ds.cleaner)


class TestResolveCallable:
    def test_resolves_stdlib_function(self):
        func = _resolve_callable("os.getcwd")
        assert callable(func)

    def test_invalid_module_raises(self):
        with pytest.raises(ModuleNotFoundError):
            _resolve_callable("fake_module.fake_func")

    def test_invalid_function_raises(self):
        with pytest.raises(AttributeError):
            _resolve_callable("os.nonexistent_function")


class TestDataSourcesConfig:
    def test_data_sources_loaded(self):
        assert len(DATA_SOURCES) > 0

    def test_has_expected_sources(self):
        names = {ds.name for ds in DATA_SOURCES}
        assert "measurements_external" in names
        assert "measurements_user" in names
