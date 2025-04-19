from fastapi import HTTPException, status
import logging

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse
from app.models.cassandra_models import ConversationModel

logger = logging.getLogger(__name__)

class ConversationController:
    """
    Controller for handling conversation operations
    This is a stub that students will implement
    """
    
    async def get_user_conversations(
        self, 
        user_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page
            
        Returns:
            Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        try:
            rows = await ConversationModel.get_user_conversations(user_id, page, limit)
            logger.info(f"Fetched user_conversations for user_id={user_id}: {rows}")
            total = len(rows)
            data = []
            for row in rows:
                try:
                    data.append(ConversationResponse(
                        id=row.get('conversation_id'),
                        user1_id=None,  # Not available in user_conversations, can be fetched if needed
                        user2_id=row.get('other_user_id'),
                        last_message_at=row.get('last_message_at'),
                        last_message_content=row.get('last_message_content')
                    ))
                except Exception as e:
                    logger.error(f"Error building ConversationResponse for row: {row}\n{e}")
            return PaginatedConversationResponse(
                total=total,
                page=page,
                limit=limit,
                data=data
            )
        except Exception as e:
            logger.error(f"Exception in get_user_conversations: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}"
            )
    
    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        try:
            row = await ConversationModel.get_conversation(conversation_id)
            logger.info(f"Fetched conversation for conversation_id={conversation_id}: {row}")
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            return ConversationResponse(
                id=row.get('conversation_id'),
                user1_id=row.get('user1_id'),
                user2_id=row.get('user2_id'),
                last_message_at=row.get('last_message_at'),
                last_message_content=row.get('last_message_content')
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Exception in get_conversation: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}"
            ) 