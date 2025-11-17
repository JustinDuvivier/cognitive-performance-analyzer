import pytest
import pandas as pd

from readers.csv_reader import (
    read_behavioral_csv,
    read_cognitive_csv,
    merge_user_data,
)


def test_read_behavioral_csv_parses_and_converts(tmp_path):
    csv_path = tmp_path / "behavioral.csv"
    csv_path.write_text(
        "timestamp,sleep_hours,breakfast_skipped,lunch_skipped,phone_usage,"
        "caffeine_count,steps,water_glasses,exercise\n"
        "2025-11-17 08:00:00,7.5,Y,N,10,1,1000,3,N\n"
    )

    df = read_behavioral_csv(filepath=str(csv_path))

    assert len(df) == 1
    row = df.iloc[0]
    assert pd.to_datetime("2025-11-17 08:00:00") == row["timestamp"]
    assert row["sleep_hours"] == 7.5
    # pandas uses numpy.bool_ for boolean columns, so cast to bool for comparison
    assert bool(row["breakfast_skipped"]) is True
    assert bool(row["lunch_skipped"]) is False
    assert bool(row["exercise"]) is False


def test_read_cognitive_csv_parses(tmp_path):
    csv_path = tmp_path / "cognitive.csv"
    csv_path.write_text(
        "timestamp,brain_fog_score,reaction_time_ms,verbal_memory_words\n"
        "2025-11-17 08:00:00,4,245,12\n"
    )

    df = read_cognitive_csv(filepath=str(csv_path))

    assert len(df) == 1
    row = df.iloc[0]
    assert pd.to_datetime("2025-11-17 08:00:00") == row["timestamp"]
    assert row["brain_fog_score"] == 4
    assert row["reaction_time_ms"] == 245
    assert row["verbal_memory_words"] == 12


def test_merge_user_data_on_timestamp(tmp_path):
    behavioral_path = tmp_path / "behavioral.csv"
    cognitive_path = tmp_path / "cognitive.csv"

    behavioral_path.write_text(
        "timestamp,sleep_hours,breakfast_skipped,lunch_skipped,phone_usage,"
        "caffeine_count,steps,water_glasses,exercise\n"
        "2025-11-17 08:00:00,7.5,Y,N,10,1,1000,3,N\n"
    )
    cognitive_path.write_text(
        "timestamp,brain_fog_score,reaction_time_ms,verbal_memory_words\n"
        "2025-11-17 08:00:00,4,245,12\n"
    )

    behavioral_df = read_behavioral_csv(filepath=str(behavioral_path))
    cognitive_df = read_cognitive_csv(filepath=str(cognitive_path))

    merged = merge_user_data(behavioral_df, cognitive_df)

    assert len(merged) == 1
    row = merged.iloc[0]
    assert "sleep_hours" in row
    assert "brain_fog_score" in row
    assert row["sleep_hours"] == 7.5
    assert row["brain_fog_score"] == 4


