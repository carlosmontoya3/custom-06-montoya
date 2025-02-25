"""
consumer_montoya.py

Consume json messages from a live data file.
Insert the processed messages into an SQLite database.

Database functions are in utils/utils_config module.
"""

#####################################
# Import Modules
#####################################

import json
import time
import sqlite3
import matplotlib.pyplot as plt
from collections import defaultdict
import utils.utils_config as config
from utils.utils_logger import logger

category_sentiments = defaultdict(list)
time_series = []
fig, ax = plt.subplots()
plt.ion()  # Enable interactive mode

def update_sentiment_chart():
    """Update the real-time sentiment trend chart."""
    ax.clear()

    if not time_series:
        return

    for category, sentiments in category_sentiments.items():
        x_values = time_series[-len(sentiments):]
        ax.plot(x_values, sentiments, marker="o", linestyle="-", label=category)

    ax.set_xlabel("Review Count")
    ax.set_ylabel("Sentiment Score")
    ax.set_title("Live Customer Sentiment Analysis")
    ax.legend()

    plt.draw()
    plt.pause(0.1)

def init_db(db_path):
    """Initialize the SQLite database."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS customer_reviews (
            id INTEGER PRIMARY KEY,
            message TEXT,
            customer TEXT,
            timestamp TEXT,
            category TEXT,
            sentiment REAL,
            keyword_mentioned TEXT,
            message_length INTEGER
        )
    ''')
    
    conn.commit()
    conn.close()

def insert_message(message, db_path):
    """Insert a processed message into the database."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute('''
        INSERT INTO customer_reviews (message, customer, timestamp, category, sentiment, keyword_mentioned, message_length)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (message['message'], message['customer'], message['timestamp'], message['category'],
          message['sentiment'], message['keyword_mentioned'], message['message_length']))

    conn.commit()
    conn.close()

def process_message(message):
    """Process and transform a JSON message."""
    category = message.get("category")
    sentiment = float(message.get("sentiment", 0.0))

    if category and sentiment:
        category_sentiments[category].append(sentiment)
        if len(category_sentiments[category]) > 50:
            category_sentiments[category].pop(0)

        time_series.append(len(time_series) + 1)

    return message

def consume_messages_from_file(live_data_path, db_path, interval_secs):
    """Consume new messages and update sentiment tracking."""
    while True:
        with open(live_data_path, "r") as file:
            for line in file:
                if line.strip():
                    message = json.loads(line.strip())
                    processed_message = process_message(message)
                    if processed_message:
                        insert_message(processed_message, db_path)
                        update_sentiment_chart()

        time.sleep(interval_secs)

def main():
    """Main function to run the consumer process."""
    db_path = "customer_feedback.db"
    live_data_path = config.get_live_data_path()
    interval_secs = config.get_message_interval_seconds_as_int()

    init_db(db_path)
    consume_messages_from_file(live_data_path, db_path, interval_secs)

if __name__ == "__main__":
    main()
