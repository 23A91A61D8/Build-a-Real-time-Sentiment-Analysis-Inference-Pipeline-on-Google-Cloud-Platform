"""
publisher.py

Reads text entries from sample_texts.json and publishes each one as a
Base64-encoded JSON message to the Google Cloud Pub/Sub 'sentiment-input'
topic.

Environment Variables:
    GCP_PROJECT_ID   - Your GCP project ID (required)
    PUB_SUB_TOPIC_ID - Your Pub/Sub topic ID, e.g. 'sentiment-input' (required)

Usage:
    export GCP_PROJECT_ID="avian-current-498109-b0"
    export PUB_SUB_TOPIC_ID="sentiment-input"
    python publisher.py

Author: Arepalli Venkata Lakshmi
"""

import json
import os
import time

from google.cloud import pubsub_v1

# ---------------------------------------------------------------------------
# Read configuration from environment variables (no hardcoded credentials)
# ---------------------------------------------------------------------------
project_id = os.getenv("GCP_PROJECT_ID", "avian-current-498109-b0")
topic_id = os.getenv("PUB_SUB_TOPIC_ID", "sentiment-input")


def publish_message(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    message: dict,
) -> None:
    """Publish a single message dict to a Cloud Pub/Sub topic.

    The dictionary is serialised to a UTF-8 JSON string before publishing.
    The Pub/Sub client automatically Base64-encodes the bytes on the wire.

    Args:
        publisher   (pubsub_v1.PublisherClient): Authenticated publisher client.
        topic_path  (str): Fully-qualified topic resource name,
                           e.g. 'projects/avian-current-498109-b0/topics/sentiment-input'.
        message     (dict): Payload to publish, must contain a "text" key.
    """
    data_str = json.dumps(message)
    data_bytes = data_str.encode("utf-8")

    future = publisher.publish(topic_path, data_bytes)
    message_id = future.result()  # blocks until the publish is acknowledged
    print(f"[INFO] Published message ID: {message_id} | Payload: {data_str}")


def main() -> None:
    """Entry point: validate env vars, load sample data, and publish messages."""
    # ------------------------------------------------------------------
    # 1. Validate required environment variables
    # ------------------------------------------------------------------
    if not project_id or not topic_id:
        print(
            "[ERROR] Both GCP_PROJECT_ID and PUB_SUB_TOPIC_ID environment "
            "variables must be set before running this script."
        )
        raise SystemExit(1)

    topic_path = f"projects/{project_id}/topics/{topic_id}"
    publisher_client = pubsub_v1.PublisherClient()

    # ------------------------------------------------------------------
    # 2. Load sample texts from JSON file
    # ------------------------------------------------------------------
    sample_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_texts.json")
    try:
        with open(sample_file, "r", encoding="utf-8") as file_handle:
            sample_texts = json.load(file_handle)
    except FileNotFoundError:
        print(
            f"[ERROR] '{sample_file}' not found. "
            "Please ensure sample_texts.json exists in the same directory."
        )
        raise SystemExit(1)
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Failed to parse sample_texts.json: {exc}")
        raise SystemExit(1)

    # ------------------------------------------------------------------
    # 3. Publish each entry to Pub/Sub
    # ------------------------------------------------------------------
    print(f"[INFO] Publishing {len(sample_texts)} message(s) to topic: {topic_path}")
    for entry in sample_texts:
        try:
            publish_message(publisher_client, topic_path, entry)
            time.sleep(1)  # small delay to avoid hitting API quotas
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[ERROR] Failed to publish message {entry.get('id', '?')}: {exc}")

    print("[INFO] All messages published successfully.")


if __name__ == "__main__":
    main()