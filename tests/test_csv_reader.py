import pytest
import pandas as pd

from readers.csv_reader import (
    _read_csv,
    read_behavioral_csv,
    read_cognitive_csv,
    merge_user_data,
)


class TestReadBehavioralCsv:
    @pytest.fixture
    def csv_file(self, tmp_path):
        path = tmp_path / "behavioral.csv"
        path.write_text("timestamp,sleep_hours,phone_usage,steps,screen_time_minutes,active_energy_kcal,calories_intake,protein_g,carbs_g,fat_g\n2025-11-17 08:00:00,7.5,10,1000,30,120,300,15,40,10\n")
        return path

    def test_parses_csv(self, csv_file):
        df = read_behavioral_csv(filepath=str(csv_file))
        assert len(df) == 1
        assert df.iloc[0]["sleep_hours"] == 7.5

    def test_missing_file_returns_empty(self, tmp_path):
        assert read_behavioral_csv(filepath=str(tmp_path / "missing.csv")).empty


class TestReadCognitiveCsv:
    @pytest.fixture
    def csv_file(self, tmp_path):
        path = tmp_path / "cognitive.csv"
        path.write_text("timestamp,sequence_memory_score,reaction_time_ms,verbal_memory_words\n2025-11-17 08:00:00,6,245,12\n")
        return path

    def test_parses_csv(self, csv_file):
        df = read_cognitive_csv(filepath=str(csv_file))
        assert len(df) == 1
        assert df.iloc[0]["reaction_time_ms"] == 245

    def test_missing_file_returns_empty(self, tmp_path):
        assert read_cognitive_csv(filepath=str(tmp_path / "missing.csv")).empty


class TestMergeUserData:
    def test_merges_on_common_columns(self):
        behavioral = pd.DataFrame({"timestamp": [pd.Timestamp("2025-01-01")], "person": ["A"], "sleep": [7]})
        cognitive = pd.DataFrame({"timestamp": [pd.Timestamp("2025-01-01")], "person": ["A"], "score": [10]})
        merged = merge_user_data(behavioral, cognitive)
        assert len(merged) == 1
        assert "sleep" in merged.columns and "score" in merged.columns

    def test_empty_input_returns_empty(self):
        assert merge_user_data(pd.DataFrame(), pd.DataFrame()).empty


class TestReadCsv:
    def test_adds_person_column_if_missing(self, tmp_path):
        path = tmp_path / "test.csv"
        path.write_text("timestamp,value\n2025-01-15 08:00:00,100\n")
        df = _read_csv("test.csv", str(path), "Test")
        assert df.iloc[0]["person"] == "Unknown"
