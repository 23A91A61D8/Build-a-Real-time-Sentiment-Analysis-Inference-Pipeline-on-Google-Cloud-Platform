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
# ---------------------------------------------------------------------------
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    nltk.download("vader_lexicon", quiet=True)

_analyzer = SentimentIntensityAnalyzer()
logger.info("VADER SentimentIntensityAnalyzer initialised successfully.")


def analyze_sentiment(text: str) -> dict:
    """Perform sentiment analysis on the given text using NLTK VADER.

    Args:
        text (str): The input text to analyse. Must be a non-empty string.

    Returns:
        dict: A dictionary containing:
            - text            : original input text
            - sentiment_label : 'POSITIVE', 'NEGATIVE', or 'NEUTRAL'
            - sentiment_score : compound score in the range [-1.0, 1.0]

    Raises:
        ValueError: If text is not a non-empty string.
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


def process_pubsub_message(event, context=None):
    """Cloud Function entry point triggered by a Pub/Sub message.

    Supports both:
    - GCP Cloud Function trigger (event dict with 'data' key)
    - Local testing via functions-framework HTTP requests

    Args:
        event   : The Pub/Sub event payload or Flask request object.
        context : Metadata for the event (event_id, timestamp, etc.).
    """
    # Handle functions-framework HTTP request format (local testing)
    try:
        if hasattr(event, 'get_json'):
            request_json = event.get_json(silent=True) or {}
            if 'data' in request_json:
                event = request_json
            elif 'message' in request_json:
                msg = request_json['message']
                event = {'data': msg.get('data', '')}
            else:
                event = request_json
    except Exception:
        pass

    # Validate that the event contains a 'data' field
    if not isinstance(event, dict) or 'data' not in event:
        logger.error(
            "No 'data' field found in Pub/Sub message. Event ID: %s",
            getattr(context, "event_id", "unknown"),
        )
        return "No data field found", 400

    text_to_analyze = None

    try:
        # Decode Base64-encoded message payload
        raw_data = event["data"]
        if isinstance(raw_data, bytes):
            message_data = base64.b64decode(raw_data).decode("utf-8")
        else:
            message_data = base64.b64decode(raw_data.encode("utf-8")).decode("utf-8")

        logger.info(
            "Received Pub/Sub message. Event ID: %s",
            getattr(context, "event_id", "unknown"),
        )

        # Parse JSON payload
        message_json = json.loads(message_data)

        # Extract the 'text' field
        text_to_analyze = message_json.get("text")
        message_id = message_json.get("id", "unknown")

        if not text_to_analyze:
            logger.warning(
                "Pub/Sub message is missing the 'text' field. "
                "Message ID: %s | Event ID: %s",
                message_id,
                getattr(context, "event_id", "unknown"),
            )
            return "Missing text field", 200

        # Run sentiment analysis
        sentiment_result = analyze_sentiment(text_to_analyze)

        # Log the result to Cloud Logging
        logger.info(
            "Sentiment analysis result: %s",
            json.dumps(sentiment_result),
        )

        return json.dumps(sentiment_result), 200

    except json.JSONDecodeError as exc:
        logger.error(
            "Invalid JSON in Pub/Sub message payload. Error: %s | Raw data: %s",
            exc,
            event.get("data"),
        )
        return "Invalid JSON", 400

    except ValueError as exc:
        logger.error(
            "Sentiment analysis failed due to invalid input. Error: %s | Text: %s",
            exc,
            text_to_analyze,
        )
        return "Invalid input", 400

    except Exception as exc:  # pylint: disable=broad-except
        logger.error(
            "An unexpected error occurred. Error: %s | Event ID: %s",
            exc,
            getattr(context, "event_id", "unknown"),
        )
        return "Unexpected error", 500