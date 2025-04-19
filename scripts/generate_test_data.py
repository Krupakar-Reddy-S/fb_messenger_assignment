"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    """
    logger.info("Generating test data...")
    user_ids = list(range(1, 11))
    conversation_ids = []
    now = datetime.utcnow()
    # Generate 15 unique random pairs for conversations
    pairs = set()
    while len(pairs) < 15:
        u1, u2 = random.sample(user_ids, 2)
        pair = tuple(sorted((u1, u2)))
        pairs.add(pair)
    for user1_id, user2_id in pairs:
        # Create a unique conversation_id
        conversation_id = int(now.timestamp() * 1000) + random.randint(0, 99999)
        conversation_ids.append(conversation_id)
        # Generate messages
        num_messages = random.randint(5, 20)
        messages = []
        last_message_at = now
        for i in range(num_messages):
            sender_id = user1_id if i % 2 == 0 else user2_id
            receiver_id = user2_id if i % 2 == 0 else user1_id
            created_at = now - timedelta(minutes=num_messages - i)
            content = f"Test message {i+1} from {sender_id} to {receiver_id}"
            message_id = uuid.uuid4()
            messages.append((conversation_id, created_at, message_id, sender_id, receiver_id, content))
            last_message_at = created_at
        # Insert messages
        for m in messages:
            session.execute('''
                INSERT INTO messages (conversation_id, created_at, message_id, sender_id, receiver_id, content)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', m)
        # Insert conversation
        session.execute('''
            INSERT INTO conversations (conversation_id, user1_id, user2_id, last_message_at, last_message_content)
            VALUES (%s, %s, %s, %s, %s)
        ''', (conversation_id, user1_id, user2_id, last_message_at, messages[-1][5]))
        # Insert user_conversations for both users
        for uid, oid in [(user1_id, user2_id), (user2_id, user1_id)]:
            session.execute('''
                INSERT INTO user_conversations (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
                VALUES (%s, %s, %s, %s, %s)
            ''', (uid, conversation_id, oid, last_message_at, messages[-1][5]))
    logger.info(f"Generated {len(conversation_ids)} conversations with messages")
    logger.info(f"User IDs: {user_ids}")
    logger.info(f"Conversation IDs: {conversation_ids}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 