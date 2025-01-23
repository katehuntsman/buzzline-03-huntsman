import os
import sys
import time
import pathlib
import json
import random
from datetime import datetime
from dotenv import load_dotenv
from utils.utils_producer import verify_services, create_kafka_producer, create_kafka_topic
from utils.utils_logger import logger

# Load Environment Variables
load_dotenv()

# Getter functions for environment variables
def get_kafka_topic() -> str:
    topic = os.getenv("MY_TOPIC", "unique_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("MY_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

# Custom Sentence Generator for Messages
def generate_messages():
    """
    Yield custom JSON message with dynamic sentences continuously.
    """
    message_id = 1
    authors = ["Bri", "Ryan", "Kenny", "Kate"]
    statuses = ["active", "inactive", "pending", "completed"]
    
    while True:
        # Create a dynamic message with custom sentences
        author = random.choice(authors)
        status = random.choice(statuses)
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        message = {
            "message": f"Message {message_id}: The process for {author} is currently {status}.",
            "author": author,
            "timestamp": timestamp,
            "status": status
        }
        
        logger.debug(f"Generated JSON: {message}")
        yield message
        message_id += 1
        time.sleep(1)  # Simulate interval

def main():
    """
    Main entry point for the producer.
    """
    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer(value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create the Kafka topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in generate_messages():
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")

if __name__ == "__main__":
    main()
