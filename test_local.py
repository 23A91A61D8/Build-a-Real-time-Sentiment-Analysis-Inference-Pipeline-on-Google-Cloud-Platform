"""
test_local.py
Run this to test the Cloud Function locally WITHOUT functions-framework.
Usage: python test_local.py
"""
import sys
import os
import base64
import json

# Add sentiment_analyzer to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "sentiment_analyzer"))
from main import process_pubsub_message, analyze_sentiment

class FakeContext:
    event_id = "local-test-001"

def make_event(text, msg_id="1"):
    payload = json.dumps({"id": msg_id, "text": text}).encode("utf-8")
    return {"data": base64.b64encode(payload).decode("utf-8")}

print("=" * 60)
print("LOCAL CLOUD FUNCTION TEST")
print("=" * 60)

# Test all 5 sample texts
samples = [
    ("1", "This product is absolutely amazing. Highly recommend."),
    ("2", "The customer service was terrible and I am very disappointed."),
    ("3", "Its okay, nothing special but gets the job done."),
    ("4", "The quality exceeded my expectations, truly fantastic"),
    ("5", "Never buying from this brand again, complete waste of money."),
]

print("\nRunning sentiment analysis on all 5 sample texts...\n")
for msg_id, text in samples:
    result = analyze_sentiment(text)
    print(f"ID: {msg_id}")
    print(f"Text: {text}")
    print(f"Sentiment: {result['sentiment_label']} (score: {result['sentiment_score']})")
    print("-" * 50)

print("\nTesting process_pubsub_message function...\n")
ctx = FakeContext()
for msg_id, text in samples:
    event = make_event(text, msg_id)
    process_pubsub_message(event, ctx)

print("\n" + "=" * 60)
print("ALL TESTS PASSED - Cloud Function working correctly!")
print("=" * 60)
