"""
producer_montoya.py

Stream JSON data to a file and - if available - a Kafka topic.

Example JSON message
{
    "message": "I just bought a phone! It was amazing.",
    "customer": "Alice",
    "timestamp": "2025-02-25 14:35:20",
    "category": "electronics",
    "sentiment": 0.87,
    "keyword_mentioned": "phone",
    "message_length": 42
}

Environment variables are in utils/utils_config module.
"""

#####################################
# Import Modules
#####################################

import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Stub Sentiment Analysis Function
#####################################

def assess_sentiment(text: str) -> float:
    """
    Stub for sentiment analysis.
    Returns a random float between 0 and 1 for now.
    """
    return round(random.uniform(0, 1), 2)

#####################################
# Define Message Generator
#####################################

def generate_messages():
    """
    Generate a stream of JSON messages simulating customer feedback.
    """
    ADJECTIVES = ["amazing", "terrible", "satisfactory", "disappointing", "fantastic"]
    ACTIONS = ["bought", "used", "returned", "recommended", "tried"]
    PRODUCTS = [
        "a phone", "a laptop", "a pair of shoes", "a jacket", 
        "a meal", "a vacation package", "a coffee machine", "a smartwatch"
    ]
    CUSTOMERS = ["Gage", "Ben", "Lyman", "Chris", "Kyle"]
    KEYWORD_CATEGORIES = {
        "phone": "electronics",
        "laptop": "electronics",
        "shoes": "fashion",
        "jacket": "fashion",
        "meal": "food",
        "vacation": "travel",
        "coffee": "kitchen",
        "smartwatch": "electronics",
    }
    
    while True:
        adjective = random.choice(ADJECTIVES)
        action = random.choice(ACTIONS)
        product = random.choice(PRODUCTS)
        customer = random.choice(CUSTOMERS)
        message_text = f"I just {action} {product}! It was {adjective}."
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Find category based on keywords
        keyword_mentioned = next(
            (word for word in KEYWORD_CATEGORIES if word in product), "other"
        )
        category = KEYWORD_CATEGORIES.get(keyword_mentioned, "other")

        # Assess sentiment
        sentiment = assess_sentiment(message_text)

        # Create JSON message
        json_message = {
            "message": message_text,
            "customer": customer,
            "timestamp": timestamp,
            "category": category,
            "sentiment": sentiment,
            "keyword_mentioned": keyword_mentioned,
            "message_length": len(message_text),
        }

        yield json_message

#####################################
# Define Main Function
#####################################

def main() -> None:

    logger.info("Starting Producer to run continuously.")

    try:
        interval_secs = config.get_message_interval_seconds_as_int()
        topic = config.get_kafka_topic()
        kafka_server = config.get_kafka_broker_address()
        live_data_path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    try:
        if live_data_path.exists():
            live_data_path.unlink()
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")

    if producer:
        try:
            create_kafka_topic(topic)
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    try:
        for message in generate_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")

            if producer:
                producer.send(topic, value=message)

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
