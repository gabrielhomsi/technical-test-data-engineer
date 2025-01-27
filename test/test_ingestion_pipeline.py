import json
import os

from src.pipelines.ingestion_pipeline import IngestionPipeline


def test_save_tracks_creates_file():
    """
    Tests the `save_tracks` method of the `IngestionPipeline` class to ensure that it
    correctly creates a file at the specified location when called.
    """
    path = "tracks.json"

    if os.path.exists(path):
        os.remove(path)

    pipeline = IngestionPipeline("")
    pipeline.save_tracks([None], path)

    assert os.path.exists(path), f"File not created at {path}"

    # Read the contents of newly-created file
    with open(path, "r") as f:
        data = json.load(f)

    assert len(data) == 1, "The file does not contain exactly one item"

    os.remove(path)


def test_save_users_creates_file():
    """
    Tests the `save_users` method of the `IngestionPipeline` class to ensure that it
    correctly creates a file at the specified location when called.
    """
    path = "users.json"

    if os.path.exists(path):
        os.remove(path)

    pipeline = IngestionPipeline("")
    pipeline.save_users([None], path)

    assert os.path.exists(path), f"File not created at {path}"

    # Read the contents of newly-created file
    with open(path, "r") as f:
        data = json.load(f)

    assert len(data) == 1, "The file does not contain exactly one item"

    os.remove(path)


def test_save_listen_history_creates_file():
    """
    Tests the `save_listen_history` method of the `IngestionPipeline` class to ensure that it
    correctly creates a file at the specified location when called.
    """
    path = "listen_history.json"

    if os.path.exists(path):
        os.remove(path)

    pipeline = IngestionPipeline("")
    pipeline.save_listen_history([None], path)

    assert os.path.exists(path), f"File not created at {path}"

    # Read the contents of newly-created file
    with open(path, "r") as f:
        data = json.load(f)

    assert len(data) == 1, "The file does not contain exactly one item"

    os.remove(path)
