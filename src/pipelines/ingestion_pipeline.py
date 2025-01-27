import json

import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class IngestionPipeline:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def ingest(self):
        logging.info("Starting data ingestion")

        tracks = self.get_tracks()
        users = self.get_users()
        listen_history = self.get_listen_history()

        self.save_tracks(tracks, "tracks.json")
        self.save_users(users, "users.json")
        self.save_listen_history(listen_history, "listen_history.json")

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

        with open(out_path, "w") as f:
            json.dump(tracks, f)

        logging.info(f"Finished saving tracks")

    def save_users(self, users: list, out_path: str):
        logging.info(f"Saving users to {out_path}")

        with open(out_path, "w") as f:
            json.dump(users, f)

        logging.info(f"Finished saving users")

    def save_listen_history(self, listen_history: list, out_path: str):
        logging.info(f"Saving listen history to {out_path}")

        with open(out_path, "w") as f:
            json.dump(listen_history, f)

        logging.info(f"Finished saving listen history")

    def __read_pages(self, endpoint: str) -> list:
        logging.info(f"Reading pages for {endpoint}")

        page = 1

        all_items = []

        while True:
            response = requests.get(url=endpoint, params={"page": page})
            response.raise_for_status()

            data = response.json()

            items, page, size, pages = data["items"], data["page"], data["size"], data["pages"]

            logging.info(f"Read {size} items from page {page}/{size} [{endpoint}]")

            assert len(items) == size

            all_items += items

            if page < data["pages"]:
                page += 1
            else:
                break

        logging.info(f"Collected a total of {len(all_items)} from {endpoint}")

        return all_items


def main():
    api_url = "http://127.0.0.1:8000"
    pipeline = IngestionPipeline(api_url)

    pipeline.ingest()


if __name__ == "__main__":
    main()
