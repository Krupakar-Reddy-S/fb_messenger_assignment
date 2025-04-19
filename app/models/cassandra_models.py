"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
import asyncio

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """
    
    @staticmethod
    async def create_message(conversation_id: int, sender_id: int, receiver_id: int, content: str, created_at: datetime, message_id=None):
        if message_id is None:
            message_id = uuid.uuid4()
        query = '''
            INSERT INTO messages (conversation_id, created_at, message_id, sender_id, receiver_id, content)
            VALUES (%(conversation_id)s, %(created_at)s, %(message_id)s, %(sender_id)s, %(receiver_id)s, %(content)s)
        '''
        params = {
            'conversation_id': conversation_id,
            'created_at': created_at,
            'message_id': message_id,
            'sender_id': sender_id,
            'receiver_id': receiver_id,
            'content': content
        }
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cassandra_client.execute, query, params)
        return message_id
    
    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 20):
        offset = (page - 1) * limit
        query = '''
            SELECT * FROM messages WHERE conversation_id = %(conversation_id)s ORDER BY created_at DESC, message_id ASC LIMIT %(limit)s
        '''
        params = {'conversation_id': conversation_id, 'limit': offset + limit}
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(None, cassandra_client.execute, query, params)
        return rows[offset:offset+limit]
    
    @staticmethod
    async def get_messages_before_timestamp(conversation_id: int, before_timestamp: datetime, page: int = 1, limit: int = 20):
        offset = (page - 1) * limit
        query = '''
            SELECT * FROM messages WHERE conversation_id = %(conversation_id)s AND created_at < %(before_timestamp)s ORDER BY created_at DESC, message_id ASC LIMIT %(limit)s
        '''
        params = {'conversation_id': conversation_id, 'before_timestamp': before_timestamp, 'limit': offset + limit}
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(None, cassandra_client.execute, query, params)
        return rows[offset:offset+limit]


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """
    
    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 20):
        offset = (page - 1) * limit
        query = '''
            SELECT * FROM user_conversations WHERE user_id = %(user_id)s ORDER BY last_message_at DESC, conversation_id ASC LIMIT %(limit)s
        '''
        params = {'user_id': user_id, 'limit': offset + limit}
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(None, cassandra_client.execute, query, params)
        return rows[offset:offset+limit]
    
    @staticmethod
    async def get_conversation(conversation_id: int):
        query = '''
            SELECT * FROM conversations WHERE conversation_id = %(conversation_id)s
        '''
        params = {'conversation_id': conversation_id}
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(None, cassandra_client.execute, query, params)
        return rows[0] if rows else None
    
    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int):
        # Try to find an existing conversation
        query = '''
            SELECT conversation_id FROM conversations WHERE (user1_id = %(user1_id)s AND user2_id = %(user2_id)s) OR (user1_id = %(user2_id)s AND user2_id = %(user1_id)s) LIMIT 1
        '''
        params = {'user1_id': user1_id, 'user2_id': user2_id}
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(None, cassandra_client.execute, query, params)
        if rows:
            return rows[0]['conversation_id']
        # If not found, create a new conversation
        import time
        import random
        conversation_id = int(time.time() * 1000) + random.randint(0, 999)
        insert_query = '''
            INSERT INTO conversations (conversation_id, user1_id, user2_id, last_message_at, last_message_content)
            VALUES (%(conversation_id)s, %(user1_id)s, %(user2_id)s, %(last_message_at)s, %(last_message_content)s)
        '''
        now = datetime.now(datetime.UTC)
        insert_params = {
            'conversation_id': conversation_id,
            'user1_id': user1_id,
            'user2_id': user2_id,
            'last_message_at': now,
            'last_message_content': ''
        }
        await loop.run_in_executor(None, cassandra_client.execute, insert_query, insert_params)
        return conversation_id 