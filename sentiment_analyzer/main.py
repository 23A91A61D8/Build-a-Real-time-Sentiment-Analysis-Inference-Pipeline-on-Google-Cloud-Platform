"""
sentiment_analyzer/main.py

Google Cloud Function entry point for real-time sentiment analysis.
Triggered by messages published to the 'sentiment-input' Pub/Sub topic.
Uses NLTK VADER for lightweight, efficient sentiment inference.

Author: Arepalli Venkata Lakshmi
"""

import os
import base64
import json
import logging

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global model initialisation (warm-start optimisation)
# Loaded once per Cloud Function instance to avoid repeated cold-start costs.
# ---------------------------------------------------------------------------
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    nltk.download("vader_lexicon", quiet=True)

_analyzer = SentimentIntensityAnalyzer()
logger.info("VADER SentimentIntensityAnalyzer initialised successfully.")


# ---------------------------------------------------------------------------
# Core sentiment analysis function
# ---------------------------------------------------------------------------

def analyze_sentiment(text: str) -> dict:
    """Perform sentiment analysis on the given text using NLTK VADER.

    Args:
        text (str): The input text to analyse. Must be a non-empty string.

    Returns:
        dict: A dictionary containing:
            - ``text``            : original input text
            - ``sentiment_label`` : 'POSITIVE', 'NEGATIVE', or 'NEUTRAL'
            - ``sentiment_score`` : compound score in the range [-1.0, 1.0]

    Raises:
        ValueError: If *text* is not a non-empty string.
    """
    if not text or not isinstance(text, str) or not text.strip():
        raise ValueError("Input text must be a non-empty string.")

    scores = _analyzer.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "POSITIVE"
    elif compound <= -0.05:
        label = "NEGATIVE"
    else:
        label = "NEUTRAL"

    return {
        "text": text,
        "sentiment_label": label,
        "sentiment_score": compound,
    }


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------

def process_pubsub_message(event: dict, context) -> None:
    """Cloud Function entry point triggered by a Pub/Sub message.

    The message payload must be a Base64-encoded JSON object with at least
    a ``"text"`` field, e.g.::

        {"id": "1", "text": "This product is amazing!"}

    Sentiment analysis results and errors are written to Cloud Logging.

    Args:
        event   (dict): The Pub/Sub event payload delivered by GCP.
        context      : Metadata for the event (event_id, timestamp, etc.).
    """
    # ------------------------------------------------------------------
    # 1. Validate that the event contains a 'data' field
    # ------------------------------------------------------------------
    if "data" not in event:
        logger.error(
            "No 'data' field found in Pub/Sub message. "
            "Event ID: %s",
            getattr(context, "event_id", "unknown"),
        )
        return

    text_to_analyze = None  # kept in scope so the except block can log it

    try:
        # ----------------------------------------------------------------
        # 2. Decode Base64-encoded message payload
        # ----------------------------------------------------------------
        message_data = base64.b64decode(event["data"]).decode("utf-8")
        logger.info(
            "Received Pub/Sub message. Event ID: %s",
            getattr(context, "event_id", "unknown"),
        )

        # ----------------------------------------------------------------
        # 3. Parse JSON payload
        # ----------------------------------------------------------------
        message_json = json.loads(message_data)

        # ----------------------------------------------------------------
        # 4. Extract the 'text' field
        # ----------------------------------------------------------------
        text_to_analyze = message_json.get("text")
        message_id = message_json.get("id", "unknown")

        if not text_to_analyze:
            logger.warning(
                "Pub/Sub message is missing the 'text' field. "
                "Message ID: %s | Event ID: %s",
                message_id,
                getattr(context, "event_id", "unknown"),
            )
            return

        # ----------------------------------------------------------------
        # 5. Run sentiment analysis
        # ----------------------------------------------------------------
        sentiment_result = analyze_sentiment(text_to_analyze)

        # ----------------------------------------------------------------
        # 6. Log the result to Cloud Logging (INFO level)
        # ----------------------------------------------------------------
        logger.info(
            "Sentiment analysis result: %s",
            json.dumps(sentiment_result),
        )

    except json.JSONDecodeError as exc:
        logger.error(
            "Invalid JSON in Pub/Sub message payload. "
            "Error: %s | Raw data: %s",
            exc,
            event.get("data"),
        )

    except ValueError as exc:
        logger.error(
            "Sentiment analysis failed due to invalid input. "
            "Error: %s | Text: %s",
            exc,
            text_to_analyze,
        )

    except Exception as exc:  # pylint: disable=broad-except
        logger.error(
            "An unexpected error occurred during message processing. "
            "Error: %s | Event ID: %s",
            exc,
            getattr(context, "event_id", "unknown"),
        )
