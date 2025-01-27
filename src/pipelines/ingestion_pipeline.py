import json
import os

import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # output to console
        logging.FileHandler("ingestion_pipeline.log", mode="a")  # output to .log file (append mode)
    ]
)


class IngestionPipeline:
    """
    This class implements a pipeline for ingesting data from an API.

    It fetches data from multiple API endpoints ("/tracks", "/users", and "/listen_history"),
    and saves the data in JSON files.
    """

    def __init__(self, api_url: str):
        self.api_url = api_url
        self.timeout = 10  # 10 seconds

        self.tracks_out_path = "tracks.json"
        self.users_out_path = "users.json"
        self.listen_history_out_path = "listen_history.json"

    def ingest(self):
        logging.info("Starting data ingestion")

        tracks = self.get_tracks()
        users = self.get_users()
        listen_history = self.get_listen_history()

        self.save_tracks(tracks, self.tracks_out_path)
        self.save_users(users, self.users_out_path)
        self.save_listen_history(listen_history, self.listen_history_out_path)

        logging.info("Data ingestion completed")

    def get_tracks(self) -> list:
        logging.info("Fetching tracks data")

        tracks = self.__read_pages(f"{self.api_url}/tracks")

        logging.info("Finished fetching tracks data")

        return tracks

    def get_users(self) -> list:
        logging.info("Fetching users data")

        users = self.users = self.__read_pages(f"{self.api_url}/users")

        logging.info("Finished fetching users data")

        return users

    def get_listen_history(self) -> list:
        logging.info("Fetching listen history data")

        listen_history = self.__read_pages(f"{self.api_url}/listen_history")

        logging.info("Finished fetching listen history data")

        return listen_history

    def save_tracks(self, tracks: list, out_path: str):
        logging.info(f"Saving tracks to {out_path}")

        self.__save_data(tracks, out_path)

        logging.info(f"Finished saving tracks")

    def save_users(self, users: list, out_path: str):
        logging.info(f"Saving users to {out_path}")

        self.__save_data(users, out_path)

        logging.info(f"Finished saving users")

    def save_listen_history(self, listen_history: list, out_path: str):
        logging.info(f"Saving listen history to {out_path}")

        self.__save_data(listen_history, out_path)

        logging.info(f"Finished saving listen history")

    def __read_pages(self, endpoint: str) -> list:
        logging.info(f"Reading pages for {endpoint}")

        page = 1

        all_items = []

        while True:
            try:
                response = requests.get(url=endpoint, params={"page": page}, timeout=self.timeout)
                response.raise_for_status()

                data = response.json()

                items, page, size, pages = data["items"], data["page"], data["size"], data["pages"]

                logging.info(f"Read {size} items from page {page}/{pages} [{endpoint}]")

                all_items += items

                if page < data["pages"]:
                    page += 1
                else:
                    break
            except Exception as e:
                logging.error(f"Request failed for {endpoint} at page {page}: {e}")
                raise

        logging.info(f"Collected a total of {len(all_items)} from {endpoint}")

        return all_items

    def __save_data(self, data: list, out_path: str):
        if os.path.exists(out_path):
            logging.warning(f"File {out_path} already exists. Overwriting")

        with open(out_path, "w") as f:
            json.dump(data, f)


def main():
    api_url = "http://127.0.0.1:8000"

    pipeline = IngestionPipeline(api_url)
    pipeline.ingest()


if __name__ == "__main__":
    main()
