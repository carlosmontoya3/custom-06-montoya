"""
consumer_montoya.py

Consumes JSON messages from a live data file and inserts processed messages into an SQLite database.
Additionally, it tracks sentiment trends and updates a real-time visualization.

Database functions are in the utils/utils_config module.
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

#####################################
# Initialize Data Structures for Visualization
#####################################

# Dictionary to store sentiment scores per category
category_sentiments = defaultdict(list)

# List to track the number of processed messages for x-axis values
time_series = []

# Initialize the plot for real-time sentiment analysis
fig, ax = plt.subplots()
plt.ion()  # Enable interactive mode for live updates

def update_sentiment_chart():
    """
    Updates the real-time sentiment trend chart with the latest data.
    This function clears the plot and re-renders the latest sentiment scores.
    """
    ax.clear()

    if not time_series:
        return  # Skip updating if no messages have been processed yet

    # Plot sentiment trends for each category
    for category, sentiments in category_sentiments.items():
        x_values = time_series[-len(sentiments):]  # Keep x-axis aligned with data length
        ax.plot(x_values, sentiments, marker="o", linestyle="-", label=category)

    ax.set_xlabel("Review Count")
    ax.set_ylabel("Sentiment Score")
    ax.set_title("Live Customer Sentiment Analysis")
    ax.legend()

    plt.draw()
    plt.pause(0.1)  # Pause briefly to allow real-time updates

#####################################
# Database Initialization
#####################################

def init_db(db_path):
    """
    Initializes the SQLite database by creating the necessary table if it doesn't exist.
    The table stores customer reviews and related metadata.
    """
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

#####################################
# Insert Processed Message into Database
#####################################

def insert_message(message, db_path):
    """
    Inserts a processed customer review message into the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute('''
        INSERT INTO customer_reviews (message, customer, timestamp, category, sentiment, keyword_mentioned, message_length)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (message['message'], message['customer'], message['timestamp'], message['category'],
          message['sentiment'], message['keyword_mentioned'], message['message_length']))

    conn.commit()
    conn.close()

#####################################
# Process Incoming Message
#####################################

def process_message(message):
    """
    Processes an incoming JSON message, extracting relevant data and updating sentiment tracking.
    """
    category = message.get("category")
    sentiment = float(message.get("sentiment", 0.0))  # Ensure sentiment is a valid float

    if category and sentiment:
        category_sentiments[category].append(sentiment)

        # Maintain a rolling window of the last 50 sentiment values for each category
        if len(category_sentiments[category]) > 50:
            category_sentiments[category].pop(0)

        # Increment the message count for x-axis tracking
        time_series.append(len(time_series) + 1)

    return message

#####################################
# Consume Messages from File
#####################################

def consume_messages_from_file(live_data_path, db_path, interval_secs):
    """
    Reads messages from a live data file, processes them, and updates the database and sentiment chart.
    Runs continuously with a short delay between reads.
    """
    while True:
        with open(live_data_path, "r") as file:
            for line in file:
                if line.strip():  # Ensure the line isn't empty
                    message = json.loads(line.strip())  # Convert JSON string to dictionary
                    processed_message = process_message(message)

                    if processed_message:
                        insert_message(processed_message, db_path)
                        update_sentiment_chart()  # Refresh the sentiment visualization

        time.sleep(interval_secs)  # Wait before reading new messages

#####################################
# Define Main Function
#####################################

def main():
    """
    Initializes the database and starts consuming messages from the live data file.
    """
    db_path = "customer_feedback.db"
    live_data_path = config.get_live_data_path()
    interval_secs = config.get_message_interval_seconds_as_int()

    init_db(db_path)  # Ensure the database is set up
    consume_messages_from_file(live_data_path, db_path, interval_secs)  # Start processing messages

if __name__ == "__main__":
    main()
