"""
Conversation Logger - Log full agent conversations to S3 for analysis

Stores complete conversations with:
- User questions and agent responses
- Tool calls made
- Timestamps and metadata
- Optional ratings/annotations

Format: JSONL (JSON Lines) for easy appending and analysis
"""

import boto3
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from data_pipeline import config


class ConversationLogger:
    """Log agent conversations to S3 for later analysis and rating"""

    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        self.bucket_name = config.aws_bucket_name
        self.conversations_prefix = "agent_conversations/"

    def log_conversation_turn(
        self,
        session_id: str,
        user_question: str,
        agent_response: str,
        tool_calls: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        rating: Optional[int] = None,
        user: str = "anonymous"
    ) -> bool:
        """
        Log a single conversation turn (question + response) to S3

        Args:
            session_id: Unique identifier for this conversation session
            user_question: The question the user asked
            agent_response: The agent's response
            tool_calls: List of tools called (optional) - [{"tool": "name", "params": {...}}]
            metadata: Additional metadata (optional) - {"model": "claude-3", "response_time": 2.5, etc.}
            rating: User rating 1-5 (optional)
            user: User identifier (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            timestamp = datetime.now()
            date_str = timestamp.strftime("%Y-%m-%d")

            # Create conversation turn entry
            turn_entry = {
                "session_id": session_id,
                "timestamp": timestamp.isoformat(),
                "user": user,
                "question": user_question,
                "response": agent_response,
                "tool_calls": tool_calls or [],
                "metadata": metadata or {},
                "rating": rating,
                "date": date_str
            }

            # File path: agent_conversations/YYYY-MM-DD_conversations.jsonl
            file_key = f"{self.conversations_prefix}{date_str}_conversations.jsonl"

            # Try to get existing file
            try:
                response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                existing_content = response['Body'].read().decode('utf-8')
            except self.s3.exceptions.NoSuchKey:
                existing_content = ""

            # Append new conversation turn (JSONL format - one JSON per line)
            new_line = json.dumps(turn_entry) + "\n"
            updated_content = existing_content + new_line

            # Upload back to S3
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=updated_content,
                ContentType='application/x-ndjson'
            )

            return True

        except Exception as e:
            print(f"Error logging conversation: {e}")
            return False

    def log_full_session(
        self,
        session_id: str,
        conversation_history: List[Dict[str, str]],
        metadata: Optional[Dict[str, Any]] = None,
        user: str = "anonymous"
    ) -> bool:
        """
        Log an entire conversation session at once

        Args:
            session_id: Unique identifier for this session
            conversation_history: List of {"role": "user/assistant", "content": "..."}
            metadata: Session-level metadata (optional)
            user: User identifier (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            timestamp = datetime.now()
            date_str = timestamp.strftime("%Y-%m-%d")

            # Create session entry
            session_entry = {
                "session_id": session_id,
                "timestamp": timestamp.isoformat(),
                "user": user,
                "conversation": conversation_history,
                "turn_count": len([m for m in conversation_history if m["role"] == "user"]),
                "metadata": metadata or {},
                "date": date_str
            }

            # File path: agent_conversations/sessions/YYYY-MM-DD/session_{id}.json
            file_key = f"{self.conversations_prefix}sessions/{date_str}/session_{session_id}.json"

            # Upload to S3
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=json.dumps(session_entry, indent=2),
                ContentType='application/json'
            )

            return True

        except Exception as e:
            print(f"Error logging full session: {e}")
            return False

    def add_rating_to_turn(
        self,
        session_id: str,
        timestamp: str,
        rating: int,
        feedback: Optional[str] = None
    ) -> bool:
        """
        Add a rating to a previously logged conversation turn

        Args:
            session_id: Session identifier
            timestamp: ISO format timestamp of the turn to rate
            rating: Rating 1-5
            feedback: Optional text feedback

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Extract date from timestamp
            dt = datetime.fromisoformat(timestamp)
            date_str = dt.strftime("%Y-%m-%d")

            # Get the conversations file for that date
            file_key = f"{self.conversations_prefix}{date_str}_conversations.jsonl"

            try:
                response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                content = response['Body'].read().decode('utf-8')
            except self.s3.exceptions.NoSuchKey:
                print(f"No conversations found for date {date_str}")
                return False

            # Parse, update the matching turn, and rewrite
            lines = content.strip().split('\n')
            updated_lines = []
            found = False

            for line in lines:
                if not line:
                    continue

                turn = json.loads(line)

                # Match by session_id and timestamp
                if turn.get("session_id") == session_id and turn.get("timestamp") == timestamp:
                    turn["rating"] = rating
                    if feedback:
                        turn["rating_feedback"] = feedback
                    found = True

                updated_lines.append(json.dumps(turn))

            if not found:
                print(f"No matching conversation turn found")
                return False

            # Write back to S3
            updated_content = '\n'.join(updated_lines) + '\n'
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=updated_content,
                ContentType='application/x-ndjson'
            )

            return True

        except Exception as e:
            print(f"Error adding rating: {e}")
            return False

    def get_conversations_by_date(
        self,
        date: str,
        session_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve conversations for a specific date

        Args:
            date: Date string in YYYY-MM-DD format
            session_id: Optional filter by session_id

        Returns:
            list: List of conversation turn dictionaries
        """
        try:
            file_key = f"{self.conversations_prefix}{date}_conversations.jsonl"

            response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
            content = response['Body'].read().decode('utf-8')

            conversations = []
            for line in content.strip().split('\n'):
                if line:
                    turn = json.loads(line)
                    if session_id is None or turn.get("session_id") == session_id:
                        conversations.append(turn)

            return conversations

        except self.s3.exceptions.NoSuchKey:
            print(f"No conversations found for date {date}")
            return []
        except Exception as e:
            print(f"Error retrieving conversations: {e}")
            return []

    def get_recent_conversations(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Retrieve conversations from the last N days

        Args:
            days: Number of days to look back

        Returns:
            list: List of all conversation turns from the period
        """
        try:
            # List all conversation files
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.conversations_prefix
            )

            if 'Contents' not in response:
                return []

            conversations = []

            # Get conversation JSONL files (not session subdirectories)
            files = [
                f for f in response['Contents']
                if f['Key'].endswith('_conversations.jsonl')
            ]

            # Sort by date (most recent first)
            files = sorted(files, key=lambda x: x['Key'], reverse=True)

            # Get last N days of files
            for file_obj in files[:days]:
                file_key = file_obj['Key']

                # Download and parse JSONL
                obj_response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                content = obj_response['Body'].read().decode('utf-8')

                # Parse each line as JSON
                for line in content.strip().split('\n'):
                    if line:
                        conversations.append(json.loads(line))

            return conversations

        except Exception as e:
            print(f"Error retrieving recent conversations: {e}")
            return []

    def get_sessions_by_rating(
        self,
        min_rating: int = 1,
        max_rating: int = 5,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Retrieve conversations filtered by rating

        Args:
            min_rating: Minimum rating (1-5)
            max_rating: Maximum rating (1-5)
            days: Number of days to look back

        Returns:
            list: List of rated conversation turns
        """
        conversations = self.get_recent_conversations(days)

        return [
            turn for turn in conversations
            if turn.get("rating") is not None
            and min_rating <= turn.get("rating") <= max_rating
        ]
