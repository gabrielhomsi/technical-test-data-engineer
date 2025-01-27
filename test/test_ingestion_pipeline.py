import json
import os
from unittest.mock import patch, Mock, MagicMock

from src.pipelines.ingestion_pipeline import IngestionPipeline


@patch("src.pipelines.ingestion_pipeline.requests.get")
def test_get_tracks(mock_get: Mock):
    """
    Tests the `get_tracks` method of the `IngestionPipeline` class by mocking the `requests.get` function to simulate
    API calls. Validates that the correct number of tracks is returned and their attributes match expected values.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {"id": 46720, "name": "however", "artist": "Steven Delacruz", "songwriters": "Robert Owens",
             "duration": "10:54", "genres": "attorney", "album": "walk", "created_at": "2023-06-06T02:51:33.637006",
             "updated_at": "2024-07-04T14:05:03.752341"},
            {"id": 37192, "name": "take", "artist": "Brian Harris", "songwriters": "Vanessa Castro",
             "duration": "37:10", "genres": "consider", "album": "himself", "created_at": "2023-05-10T00:33:08.385910",
             "updated_at": "2024-12-31T05:04:05.076131"}],
        "page": 1,
        "size": 2,
        "pages": 1
    }

    mock_get.return_value = mock_response

    # Initialize IngestionPipeline with a mock API
    pipeline = IngestionPipeline(api_url="http://mock-api.com")

    tracks = pipeline.get_tracks()

    assert len(tracks) == 2
    assert tracks[0]["name"] == "however"
    assert tracks[1]["name"] == "take"

    mock_get.assert_called_once_with(url="http://mock-api.com/tracks", params={"page": 1}, timeout=pipeline.timeout)


@patch("src.pipelines.ingestion_pipeline.requests.get")
def test_get_users(mock_get: Mock):
    """
    Tests the `get_users` method of the `IngestionPipeline` class by mocking the `requests.get` function to simulate
    API calls. Validates that the correct number of users is returned and their attributes match expected values.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {"id": 73090, "first_name": "Melissa", "last_name": "Morrow", "email": "brice@example.net",
             "gender": "Gender nonconforming", "favorite_genres": "Funk", "created_at": "2023-04-26T17:39:33.261407",
             "updated_at": "2024-06-21T14:17:34.956630"},
            {"id": 36917, "first_name": "Kristen", "last_name": "Spencer", "email": "acraig@example.com",
             "gender": "Male", "favorite_genres": "Pop", "created_at": "2024-05-23T22:38:52.442277",
             "updated_at": "2024-07-06T17:05:55.197747"},
            {"id": 62821, "first_name": "Robert", "last_name": "Malone", "email": "esoto@example.com",
             "gender": "Genderqueer", "favorite_genres": "Jazz", "created_at": "2024-09-26T01:06:42.192228",
             "updated_at": "2024-10-07T14:05:01.622347"},
        ],
        "page": 1,
        "size": 3,
        "pages": 1
    }

    mock_get.return_value = mock_response

    # Initialize IngestionPipeline with a mock API
    pipeline = IngestionPipeline(api_url="http://mock-api.com")

    users = pipeline.get_users()

    assert len(users) == 3

    assert users[0]["first_name"] == "Melissa"
    assert users[1]["first_name"] == "Kristen"
    assert users[2]["first_name"] == "Robert"

    mock_get.assert_called_once_with(url="http://mock-api.com/users", params={"page": 1}, timeout=pipeline.timeout)


@patch("src.pipelines.ingestion_pipeline.requests.get")
def test_get_listen_history(mock_get: Mock):
    """
    Tests the `get_listen_history` method of the `IngestionPipeline` class by mocking the `requests.get`
    function to simulate API calls. Validates that the correct number of listen_history records is returned
    and their attributes match expected values.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {"user_id": 75236, "items": [82798, 10813, 50319, 45822, 68713], "created_at": "2025-01-12T21:14:07.269795",
             "updated_at": "2025-01-13T16:00:24.960206"},
            {"user_id": 51194, "items": [5976, 67615, 63664, 51881, 70712], "created_at": "2023-08-17T02:09:49.128559",
             "updated_at": "2024-08-27T06:06:51.643695"},
            {"user_id": 87713, "items": [22514, 73041, 42911, 23336, 15804], "created_at": "2024-04-28T15:45:52.172240",
             "updated_at": "2024-10-03T17:36:15.697994"},
            {"user_id": 67491, "items": [66336, 11112, 6466, 91129, 85440], "created_at": "2024-11-03T11:30:20.319140",
             "updated_at": "2024-12-29T12:01:41.458460"},
        ],
        "page": 1,
        "size": 3,
        "pages": 1
    }

    mock_get.return_value = mock_response

    # Initialize IngestionPipeline with a mock API
    pipeline = IngestionPipeline(api_url="http://mock-api.com")

    listen_history = pipeline.get_listen_history()

    assert len(listen_history) == 4

    assert listen_history[0]["user_id"] == 75236
    assert listen_history[1]["user_id"] == 51194
    assert listen_history[2]["user_id"] == 87713
    assert listen_history[3]["user_id"] == 67491

    mock_get.assert_called_once_with(url="http://mock-api.com/listen_history", params={"page": 1},
                                     timeout=pipeline.timeout)


def test_save_tracks():
    """
    Tests the functionality of saving tracks to a JSON file using the `save_tracks` method
    of the `IngestionPipeline` class.

    The test validates the following:
    1. A file is created at the specified path after calling the `save_tracks` method.
    2. The contents of the created file match the expected track data provided as input.
    """
    path = "tracks.json"

    if os.path.exists(path):
        os.remove(path)

    tracks = [
        {"id": 46720, "name": "however", "artist": "Steven Delacruz", "songwriters": "Robert Owens",
         "duration": "10:54", "genres": "attorney", "album": "walk", "created_at": "2023-06-06T02:51:33.637006",
         "updated_at": "2024-07-04T14:05:03.752341"},
        {"id": 37192, "name": "take", "artist": "Brian Harris", "songwriters": "Vanessa Castro",
         "duration": "37:10", "genres": "consider", "album": "himself", "created_at": "2023-05-10T00:33:08.385910",
         "updated_at": "2024-12-31T05:04:05.076131"}
    ]

    pipeline = IngestionPipeline("fake-url")
    pipeline.save_tracks(tracks, path)

    assert os.path.exists(path), f"File not created at {path}"

    # Read the contents of newly-created file
    with open(path, "r") as f:
        data = json.load(f)

    assert len(data) == 2, "The file does not contain exactly two items"

    assert data[0]["name"] == "however"
    assert data[0]["artist"] == "Steven Delacruz"
    assert data[1]["name"] == "take"
    assert data[1]["artist"] == "Brian Harris"

    os.remove(path)


def test_save_users():
    """
    Tests the functionality of saving users to a JSON file using the `save_users` method
    of the `IngestionPipeline` class.

    The test validates the following:
    1. A file is created at the specified path after calling the `save_users` method.
    2. The contents of the created file match the expected user data provided as input.
    """
    path = "users.json"

    if os.path.exists(path):
        os.remove(path)

    users = [
        {"id": 73090, "first_name": "Melissa", "last_name": "Morrow", "email": "brice@example.net",
         "gender": "Gender nonconforming", "favorite_genres": "Funk", "created_at": "2023-04-26T17:39:33.261407",
         "updated_at": "2024-06-21T14:17:34.956630"},
        {"id": 36917, "first_name": "Kristen", "last_name": "Spencer", "email": "acraig@example.com",
         "gender": "Male", "favorite_genres": "Pop", "created_at": "2024-05-23T22:38:52.442277",
         "updated_at": "2024-07-06T17:05:55.197747"},
        {"id": 62821, "first_name": "Robert", "last_name": "Malone", "email": "esoto@example.com",
         "gender": "Genderqueer", "favorite_genres": "Jazz", "created_at": "2024-09-26T01:06:42.192228",
         "updated_at": "2024-10-07T14:05:01.622347"},
    ]

    pipeline = IngestionPipeline("fake-url")
    pipeline.save_users(users, path)

    assert os.path.exists(path), f"File not created at {path}"

    # Read the contents of newly-created file
    with open(path, "r") as f:
        data = json.load(f)

    assert len(data) == 3, "The file does not contain exactly three items"
    assert data[0]["first_name"] == "Melissa"
    assert data[1]["first_name"] == "Kristen"
    assert data[2]["first_name"] == "Robert"

    os.remove(path)


def test_save_listen_history():
    """
    Tests the functionality of saving the listen history to a JSON file using the `save_listen_history` method
    of the `IngestionPipeline` class.

    The test validates the following:
    1. A file is created at the specified path after calling the `save_listen_history` method.
    2. The contents of the created file match the expected listen history data provided as input.
    """
    path = "listen_history.json"

    if os.path.exists(path):
        os.remove(path)

    listen_history = [
        {"user_id": 75236, "items": [82798, 10813, 50319, 45822, 68713], "created_at": "2025-01-12T21:14:07.269795",
         "updated_at": "2025-01-13T16:00:24.960206"},
        {"user_id": 51194, "items": [5976, 67615, 63664, 51881, 70712], "created_at": "2023-08-17T02:09:49.128559",
         "updated_at": "2024-08-27T06:06:51.643695"},
        {"user_id": 87713, "items": [22514, 73041, 42911, 23336, 15804], "created_at": "2024-04-28T15:45:52.172240",
         "updated_at": "2024-10-03T17:36:15.697994"},
        {"user_id": 67491, "items": [66336, 11112, 6466, 91129, 85440], "created_at": "2024-11-03T11:30:20.319140",
         "updated_at": "2024-12-29T12:01:41.458460"},
    ]

    pipeline = IngestionPipeline("fake-url")
    pipeline.save_listen_history(listen_history, path)

    assert os.path.exists(path), f"File not created at {path}"

    # Read the contents of newly-created file
    with open(path, "r") as f:
        data = json.load(f)

    assert len(data) == 4, "The file does not contain exactly four items"
    assert data[0]["user_id"] == 75236
    assert data[1]["user_id"] == 51194
    assert data[2]["user_id"] == 87713
    assert data[3]["user_id"] == 67491

    os.remove(path)
