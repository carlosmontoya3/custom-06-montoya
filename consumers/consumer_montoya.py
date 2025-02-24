"""
consumer_montoya.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import pathlib
import sys
import time
import sqlite3
import matplotlib.pyplot as plt  # Added for live chart
from collections import defaultdict
from pathlib import Path
import utils.utils_config as config
from utils.utils_logger import logger

# Track sentiment trends dynamically
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
        # Ensure x and y have the same length
        if len(sentiments) > len(time_series):
            sentiments = sentiments[-len(time_series):]  # Trim y-data to match x-data
        elif len(sentiments) < len(time_series):
            x_values = time_series[-len(sentiments):]  # Trim x-data to match y-data
        else:
            x_values = time_series  # No trimming needed

        ax.plot(x_values, sentiments, marker="o", linestyle="-", label=category)

    ax.set_xlabel("Message Index")
    ax.set_ylabel("Average Sentiment")
    ax.set_title("Live Sentiment Trend by Category- Carlos Montoya III")
    ax.legend()
    
    plt.draw()
    plt.pause(0.1)


# Initialize database with renamed table
def init_db(db_path: Path):
    """Initialize the SQLite database with tables."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # Changed table name from 'messages' → 'sentiment_messages'
        c.execute('''
            CREATE TABLE IF NOT EXISTS sentiment_messages (
                id INTEGER PRIMARY KEY,
                message TEXT,
                author TEXT,
                timestamp TEXT,
                category TEXT,
                sentiment REAL,
                keyword_mentioned TEXT,
                message_length INTEGER
            )
        ''')

        # Keep tracking sentiment per author
        c.execute('''
            CREATE TABLE IF NOT EXISTS author_sentiment (
                author TEXT PRIMARY KEY,
                average_sentiment REAL
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {db_path}")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")
        sys.exit(1)

# Insert messages into the updated database
def insert_message(message: dict, db_path: Path):
    """Insert a processed message into the 'sentiment_messages' table."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        c.execute('''
            INSERT INTO sentiment_messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (message['message'], message['author'], message['timestamp'], message['category'], 
              message['sentiment'], message['keyword_mentioned'], message['message_length']))

        conn.commit()
        conn.close()
        logger.info(f"Message inserted for author {message['author']}")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message: {e}")

# Batch update of author sentiment
def calculate_average_sentiment(db_path: Path):
    """Calculate and update average sentiment per author."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        c.execute('''
            SELECT author, AVG(sentiment) AS avg_sentiment
            FROM sentiment_messages
            GROUP BY author
        ''')
        
        authors_avg_sentiment = c.fetchall()
        logger.info(f"Average Sentiment Data: {authors_avg_sentiment}")

        for author, avg_sentiment in authors_avg_sentiment:
            c.execute('''
                INSERT OR REPLACE INTO author_sentiment (author, average_sentiment)
                VALUES (?, ?)
            ''', (author, avg_sentiment))

        conn.commit()
        conn.close()
        logger.info("Average sentiment for authors updated.")
    except Exception as e:
        logger.error(f"ERROR: Failed to calculate average sentiment: {e}")

# Process message and update trend data
def process_message(message: str) -> dict:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.
    """
    try:
        category = message.get("category")
        sentiment = float(message.get("sentiment", 0.0))

        # Update tracking lists for real-time visualization
        if category and sentiment:
            category_sentiments[category].append(sentiment)
            if len(category_sentiments[category]) > 50:
                category_sentiments[category].pop(0)  # Keep only recent 50 messages

            time_series.append(len(time_series) + 1)

        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": category,
            "sentiment": sentiment,
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

# Read new messages and update sentiment chart
def consume_messages_from_file(live_data_path, db_path, interval_secs, last_position, batch_size=10):
    """Consume new messages from a file and update sentiment tracking."""
    logger.info("Starting message consumption...")
    
    batch_count = 0

    while True:
        try:
            with open(live_data_path, "r") as file:
                file.seek(last_position)
                for line in file:
                    if line.strip():
                        message = json.loads(line.strip())
                        processed_message = process_message(message)
                        if processed_message:
                            insert_message(processed_message, db_path)

                            batch_count += 1
                            if batch_count % batch_size == 0:
                                calculate_average_sentiment(db_path)  # Update every N messages
                                update_sentiment_chart()  # Refresh the live chart

                last_position = file.tell()

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            sys.exit(10)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

        time.sleep(interval_secs)

def main():
    """
    Main function to run the consumer process.
    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path_str: str = config.get_live_data_path()
        live_data_path: Path = Path(live_data_path_str)
        
        # Ensure "data" directory exists
        data_folder = Path("data")
        data_folder.mkdir(exist_ok=True)  # Create 'data' if it doesn't exist

        # Set the database path inside the "data" folder
        db_path: Path = data_folder / "sentiment.db"

        logger.info(f"SUCCESS: Read environment variables. Live data path: {live_data_path}")
        logger.info(f"Database will be stored at: {db_path}")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Initialize the database.")
    try:
        init_db(db_path)  # Initialize the database inside the "data" folder
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")
        sys.exit(2)

    logger.info("STEP 3. Begin consuming and storing messages.")
    try:
        last_position = 0
        while True:
            last_position = consume_messages_from_file(live_data_path, db_path, interval_secs, last_position)
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")

if __name__ == "__main__":
    main()
