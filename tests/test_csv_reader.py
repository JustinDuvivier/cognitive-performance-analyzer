import pytest
import pandas as pd

import readers.csv_reader


class TestReadBehavioralCsv:
    @pytest.fixture
    def csv_file(self, tmp_path):
        path = tmp_path / "behavioral.csv"
        path.write_text("timestamp,sleep_hours,phone_usage,steps,screen_time_minutes,active_energy_kcal,calories_intake,protein_g,carbs_g,fat_g\n2025-11-17 08:00:00,7.5,10,1000,30,120,300,15,40,10\n")
        return path

    def test_parses_csv(self, csv_file):
        df = readers.csv_reader.read_behavioral_csv(filepath=str(csv_file))
        assert len(df) == 1
        assert df.iloc[0]["sleep_hours"] == 7.5

    def test_missing_file_returns_empty(self, tmp_path):
        assert readers.csv_reader.read_behavioral_csv(filepath=str(tmp_path / "missing.csv")).empty


class TestReadCognitiveCsv:
    @pytest.fixture
    def csv_file(self, tmp_path):
        path = tmp_path / "cognitive.csv"
        path.write_text("timestamp,sequence_memory_score,reaction_time_ms,verbal_memory_words\n2025-11-17 08:00:00,6,245,12\n")
        return path

    def test_parses_csv(self, csv_file):
        df = readers.csv_reader.read_cognitive_csv(filepath=str(csv_file))
        assert len(df) == 1
        assert df.iloc[0]["reaction_time_ms"] == 245

    def test_missing_file_returns_empty(self, tmp_path):
        assert readers.csv_reader.read_cognitive_csv(filepath=str(tmp_path / "missing.csv")).empty


class TestMergeUserData:
    def test_merges_on_common_columns(self):
        behavioral = pd.DataFrame({"timestamp": [pd.Timestamp("2025-01-01")], "person": ["A"], "sleep": [7]})
        cognitive = pd.DataFrame({"timestamp": [pd.Timestamp("2025-01-01")], "person": ["A"], "score": [10]})
        merged = readers.csv_reader.merge_user_data(behavioral, cognitive)
        assert len(merged) == 1
        assert "sleep" in merged.columns and "score" in merged.columns

    def test_empty_input_returns_empty(self):
        assert readers.csv_reader.merge_user_data(pd.DataFrame(), pd.DataFrame()).empty


class TestReadExternalCsv:
    @pytest.fixture
    def csv_file(self, tmp_path):
        path = tmp_path / "external.csv"
        path.write_text("person,timestamp,pressure_hpa,temperature,humidity\nAlice,2025-11-17 08:00:00,1013.5,45.0,62.0\n")
        return path

    def test_parses_csv(self, csv_file):
        df = readers.csv_reader.read_external_csv(filepath=str(csv_file))
        assert len(df) == 1
        assert df.iloc[0]["pressure_hpa"] == 1013.5

    def test_missing_file_returns_empty(self, tmp_path):
        assert readers.csv_reader.read_external_csv(filepath=str(tmp_path / "missing.csv")).empty


class TestReadAllExternalData:
    def test_returns_list_of_dicts(self, tmp_path, monkeypatch):
        path = tmp_path / "external.csv"
        path.write_text("person,timestamp,pressure_hpa\nBob,2025-01-01 09:00:00,1010.0\n")
        monkeypatch.setattr("readers.csv_reader.PROJECT_ROOT", tmp_path.parent)
        
        import readers.csv_reader as csv_module
        original_read = csv_module.read_external_csv
        csv_module.read_external_csv = lambda fp=None: original_read(str(path))
        
        result = readers.csv_reader.read_all_external_data()
        csv_module.read_external_csv = original_read
        
        assert isinstance(result, list)


class TestReadCsv:
    def test_adds_person_column_if_missing(self, tmp_path):
        path = tmp_path / "test.csv"
        path.write_text("timestamp,value\n2025-01-15 08:00:00,100\n")
        df = readers.csv_reader._read_csv("test.csv", str(path), "Test")
        assert df.iloc[0]["person"] == "Unknown"
