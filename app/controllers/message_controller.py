from typing import Optional
from datetime import datetime
from fastapi import HTTPException, status
import asyncio
import logging

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse
from app.models.cassandra_models import MessageModel, ConversationModel
from app.db.cassandra_client import cassandra_client

logger = logging.getLogger(__name__)

class MessageController:
    """
    Controller for handling message operations
    This is a stub that students will implement
    """
    
    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another
        
        Args:
            message_data: The message data including content, sender_id, and receiver_id
            
        Returns:
            The created message with metadata
        
        Raises:
            HTTPException: If message sending fails
        """
        try:
            # Find or create the conversation
            conversation_id = await ConversationModel.create_or_get_conversation(
                message_data.sender_id, message_data.receiver_id
            )
            now = datetime.utcnow()
            message_id = await MessageModel.create_message(
                conversation_id=conversation_id,
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                content=message_data.content,
                created_at=now
            )
            # Update conversation metadata (last_message_at, last_message_content)
            update_query = '''
                UPDATE conversations SET last_message_at = %(last_message_at)s, last_message_content = %(last_message_content)s WHERE conversation_id = %(conversation_id)s
            '''
            update_params = {
                'last_message_at': now,
                'last_message_content': message_data.content,
                'conversation_id': conversation_id
            }
            await asyncio.get_event_loop().run_in_executor(None, cassandra_client.execute, update_query, update_params)
            # Also update user_conversations for both users
            for uid, oid in [
                (message_data.sender_id, message_data.receiver_id),
                (message_data.receiver_id, message_data.sender_id)
            ]:
                upsert_user_conv = '''
                    INSERT INTO user_conversations (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
                    VALUES (%(user_id)s, %(conversation_id)s, %(other_user_id)s, %(last_message_at)s, %(last_message_content)s)
                '''
                upsert_params = {
                    'user_id': uid,
                    'conversation_id': conversation_id,
                    'other_user_id': oid,
                    'last_message_at': now,
                    'last_message_content': message_data.content
                }
                await asyncio.get_event_loop().run_in_executor(None, cassandra_client.execute, upsert_user_conv, upsert_params)
            return MessageResponse(
                id=str(message_id),
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                content=message_data.content,
                created_at=now,
                conversation_id=conversation_id
            )
        except Exception as e:
            logger.error(f"Exception in send_message: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}"
            )
    
    async def get_conversation_messages(
        self, 
        conversation_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination
        
        Args:
            conversation_id: ID of the conversation
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            rows = await MessageModel.get_conversation_messages(conversation_id, page, limit)
            logger.info(f"Fetched messages for conversation_id={conversation_id}: {rows}")
            total = len(rows)
            data = []
            for row in rows:
                try:
                    data.append(MessageResponse(
                        id=str(row.get('message_id')),
                        sender_id=row.get('sender_id'),
                        receiver_id=row.get('receiver_id'),
                        content=row.get('content'),
                        created_at=row.get('created_at'),
                        conversation_id=row.get('conversation_id')
                    ))
                except Exception as e:
                    logger.error(f"Error building MessageResponse for row: {row}\n{e}")
            return PaginatedMessageResponse(
                total=total,
                page=page,
                limit=limit,
                data=data
            )
        except Exception as e:
            logger.error(f"Exception in get_conversation_messages: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}"
            )
    
    async def get_messages_before_timestamp(
        self, 
        conversation_id: int, 
        before_timestamp: datetime,
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination
        
        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            rows = await MessageModel.get_messages_before_timestamp(conversation_id, before_timestamp, page, limit)
            logger.info(f"Fetched messages before timestamp for conversation_id={conversation_id}: {rows}")
            total = len(rows)
            data = []
            for row in rows:
                try:
                    data.append(MessageResponse(
                        id=str(row.get('message_id')),
                        sender_id=row.get('sender_id'),
                        receiver_id=row.get('receiver_id'),
                        content=row.get('content'),
                        created_at=row.get('created_at'),
                        conversation_id=row.get('conversation_id')
                    ))
                except Exception as e:
                    logger.error(f"Error building MessageResponse for row: {row}\n{e}")
            return PaginatedMessageResponse(
                total=total,
                page=page,
                limit=limit,
                data=data
            )
        except Exception as e:
            logger.error(f"Exception in get_messages_before_timestamp: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}"
            ) 