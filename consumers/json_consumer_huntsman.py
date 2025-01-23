import os
import json
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

# Getter functions for environment variables
def get_kafka_topic() -> str:
    topic = os.getenv("MY_TOPIC", "unique_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("MY_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_rolling_window_size() -> int:
    window_size = int(os.getenv("MY_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

# Function to process a single JSON message with real-time analytics
def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    try:
        # Log the raw message
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data = json.loads(message)
        message_content = data.get("message")
        author = data.get("author")
        status = data.get("status")
        timestamp = data.get("timestamp")

        logger.info(f"Processed JSON message: {data}")

        # Ensure required fields are present
        if not all([message_content, author, status, timestamp]):
            logger.error(f"Invalid message format: {message}")
            return

        # Append message content to rolling window
        rolling_window.append(message_content)

        # Log the current state of the rolling window
        logger.info(f"Rolling Window: {list(rolling_window)}")
        
        # Real-time analytics: Alert on status patterns
        if "completed" in status.lower():
            logger.info(f"ALERT: The process for {author} has completed at {timestamp}.")
        
        # Example of detecting a specific pattern (status change to 'failed')
        if "failed" in status.lower():
            logger.error(f"ALERT: The process for {author} has failed at {timestamp}.")

        # Example of time-based alerting
        if "urgent" in message_content.lower():
            logger.warning(f"URGENT ALERT: Message from {author} at {timestamp}: {message_content}")
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

# Main consumer loop
def main():
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()

    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    # Create Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # Ensure the message is decoded only if it's bytes
            message_str = message.value
            if isinstance(message_str, bytes):  # Check if it's a byte string
                message_str = message_str.decode('utf-8')
            
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

if __name__ == "__main__":
    main()
