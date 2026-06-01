"""
write_tests.py  –  Run this once to overwrite tests/test_sentiment.py
Usage:  python write_tests.py
"""
import os

NEW_TESTS = """\
import sys
import os
import unittest
import base64
import json
from unittest.mock import MagicMock

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sentiment_analyzer"))
)
from main import analyze_sentiment, process_pubsub_message


class TestAnalyzeSentiment(unittest.TestCase):

    def test_positive_sentiment_label(self):
        result = analyze_sentiment("I absolutely love this product, its fantastic")
        self.assertEqual(result["sentiment_label"], "POSITIVE")

    def test_positive_sentiment_score(self):
        result = analyze_sentiment("I absolutely love this product, its fantastic")
        self.assertGreater(result["sentiment_score"], 0.05)

    def test_negative_sentiment_label(self):
        result = analyze_sentiment("This was a terrible experience, very disappointing.")
        self.assertEqual(result["sentiment_label"], "NEGATIVE")

    def test_negative_sentiment_score(self):
        result = analyze_sentiment("This was a terrible experience, very disappointing.")
        self.assertLess(result["sentiment_score"], -0.05)

    def test_neutral_sentiment_label(self):
        result = analyze_sentiment("The item arrived on time, as described.")
        self.assertEqual(result["sentiment_label"], "NEUTRAL")

    def test_neutral_sentiment_score(self):
        result = analyze_sentiment("The item arrived on time, as described.")
        self.assertAlmostEqual(result["sentiment_score"], 0.0, delta=0.05)

    def test_return_dict_contains_text_key(self):
        result = analyze_sentiment("Great product")
        self.assertIn("text", result)

    def test_return_dict_contains_sentiment_label_key(self):
        result = analyze_sentiment("Good service.")
        self.assertIn("sentiment_label", result)

    def test_return_dict_contains_sentiment_score_key(self):
        result = analyze_sentiment("Good service.")
        self.assertIn("sentiment_score", result)

    def test_sentiment_score_range(self):
        texts = [
            "Best product ever",
            "Absolutely horrible waste of time.",
            "It was delivered.",
        ]
        for text in texts:
            with self.subTest(text=text):
                result = analyze_sentiment(text)
                self.assertGreaterEqual(result["sentiment_score"], -1.0)
                self.assertLessEqual(result["sentiment_score"], 1.0)

    def test_positive_label_is_string(self):
        result = analyze_sentiment("I love it")
        self.assertIsInstance(result["sentiment_label"], str)

    def test_valid_label_values(self):
        valid_labels = {"POSITIVE", "NEGATIVE", "NEUTRAL"}
        texts = [
            "Excellent quality and fast delivery",
            "Worst purchase I ever made.",
            "Package arrived.",
        ]
        for text in texts:
            with self.subTest(text=text):
                result = analyze_sentiment(text)
                self.assertIn(result["sentiment_label"], valid_labels)

    def test_multiple_positive_sentences(self):
        result = analyze_sentiment(
            "Outstanding product. Exceeded all my expectations. Will definitely buy again"
        )
        self.assertEqual(result["sentiment_label"], "POSITIVE")

    def test_multiple_negative_sentences(self):
        result = analyze_sentiment(
            "Awful quality. Broke on first use. Complete rip-off. Never again"
        )
        self.assertEqual(result["sentiment_label"], "NEGATIVE")

    def test_empty_string_raises_value_error(self):
        with self.assertRaises(ValueError):
            analyze_sentiment("")

    def test_none_input_raises_value_error(self):
        with self.assertRaises(ValueError):
            analyze_sentiment(None)

    def test_integer_input_raises_value_error(self):
        with self.assertRaises(ValueError):
            analyze_sentiment(123)

    def test_list_input_raises_value_error(self):
        with self.assertRaises(ValueError):
            analyze_sentiment(["good product"])

    def test_dict_input_raises_value_error(self):
        with self.assertRaises(ValueError):
            analyze_sentiment({"text": "good product"})

    def test_whitespace_only_raises_value_error(self):
        with self.assertRaises(ValueError):
            analyze_sentiment("   ")

    def test_single_word_positive(self):
        result = analyze_sentiment("Excellent")
        self.assertIn(result["sentiment_label"], {"POSITIVE", "NEUTRAL", "NEGATIVE"})

    def test_single_word_negative(self):
        result = analyze_sentiment("Terrible")
        self.assertIn(result["sentiment_label"], {"POSITIVE", "NEUTRAL", "NEGATIVE"})


class TestAnalyzeSentimentSampleTexts(unittest.TestCase):

    def test_sample_text_1_positive(self):
        result = analyze_sentiment("This product is absolutely amazing. Highly recommend.")
        self.assertEqual(result["sentiment_label"], "POSITIVE")

    def test_sample_text_2_negative(self):
        result = analyze_sentiment("The customer service was terrible and I am very disappointed.")
        self.assertEqual(result["sentiment_label"], "NEGATIVE")

    def test_sample_text_4_positive(self):
        result = analyze_sentiment("The quality exceeded my expectations, truly fantastic")
        self.assertEqual(result["sentiment_label"], "POSITIVE")

    def test_sample_text_5_negative(self):
        result = analyze_sentiment("Never buying from this brand again, complete waste of money.")
        self.assertEqual(result["sentiment_label"], "NEGATIVE")


class TestProcessPubsubMessage(unittest.TestCase):

    def _make_context(self, event_id="test-event-123"):
        ctx = MagicMock()
        ctx.event_id = event_id
        return ctx

    def _make_event(self, payload):
        data_bytes = json.dumps(payload).encode("utf-8")
        return {"data": base64.b64encode(data_bytes).decode("utf-8")}

    def test_valid_positive_message(self):
        event = self._make_event({"id": "1", "text": "This product is absolutely amazing."})
        process_pubsub_message(event, self._make_context())

    def test_valid_negative_message(self):
        event = self._make_event({"id": "2", "text": "Terrible experience, very disappointed."})
        process_pubsub_message(event, self._make_context())

    def test_valid_neutral_message(self):
        event = self._make_event({"id": "3", "text": "The item arrived on time."})
        process_pubsub_message(event, self._make_context())

    def test_missing_data_field(self):
        process_pubsub_message({}, self._make_context())

    def test_missing_text_field(self):
        event = self._make_event({"id": "4"})
        process_pubsub_message(event, self._make_context())

    def test_invalid_json_payload(self):
        bad_bytes = b"not valid json {{{"
        event = {"data": base64.b64encode(bad_bytes).decode("utf-8")}
        process_pubsub_message(event, self._make_context())

    def test_empty_text_field(self):
        event = self._make_event({"id": "5", "text": ""})
        process_pubsub_message(event, self._make_context())

    def test_whitespace_text_field(self):
        event = self._make_event({"id": "6", "text": "   "})
        process_pubsub_message(event, self._make_context())

    def test_multiple_messages_processed(self):
        texts = [
            "I love this product",
            "Absolutely horrible.",
            "Package arrived.",
            "Best purchase ever",
            "Never again.",
        ]
        for i, text in enumerate(texts):
            event = self._make_event({"id": str(i), "text": text})
            process_pubsub_message(event, self._make_context(event_id="evt-" + str(i)))

    def test_no_id_field_in_payload(self):
        event = self._make_event({"text": "Good product"})
        process_pubsub_message(event, self._make_context())


if __name__ == "__main__":
    unittest.main()
"""

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests", "test_sentiment.py")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(NEW_TESTS)

print("SUCCESS: tests/test_sentiment.py has been updated.")
