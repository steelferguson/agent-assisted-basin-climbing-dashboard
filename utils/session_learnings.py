"""
Session Learnings - Extract and store insights from analytics conversations

Uses Claude 3 Haiku to summarize conversations and extract key learnings
"""

import boto3
import json
from datetime import datetime
from anthropic import Anthropic
from data_pipeline import config
import os


class SessionLearningsStorage:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        self.bucket_name = config.aws_bucket_name
        self.learnings_prefix = "session_learnings/"

        # Initialize Anthropic client for LLM analysis
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if api_key:
            self.anthropic = Anthropic(api_key=api_key)
        else:
            self.anthropic = None
            print("Warning: No Anthropic API key found. Session learning extraction will be disabled.")

    def extract_learnings_from_session(self, messages: list) -> dict:
        """
        Use Claude to extract key learnings from a conversation

        Args:
            messages: List of message dicts with 'role' and 'content'

        Returns:
            dict: Extracted learnings with structured data
        """
        if not self.anthropic:
            return {
                "error": "Anthropic API not available",
                "questions_asked": [],
                "insights_discovered": [],
                "charts_generated": [],
                "agent_recommendations": [],
                "user_actions": []
            }

        # Format conversation for analysis
        conversation_text = "\n".join([
            f"{msg['role'].upper()}: {msg['content']}"
            for msg in messages
        ])

        prompt = f"""Analyze this analytics conversation and extract key information.

Conversation:
{conversation_text}

Extract and return as JSON with these exact keys:
1. "questions_asked": List of questions the user asked (as strings)
2. "insights_discovered": List of key insights or findings (as strings)
3. "charts_generated": List of charts mentioned or created (as strings, can be empty)
4. "agent_recommendations": List of recommendations given (as strings, can be empty)
5. "user_actions": List of actions taken or decisions made (as strings, can be empty)

Keep each item concise (1-2 sentences max). Return ONLY valid JSON, no other text."""

        try:
            response = self.anthropic.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            # Parse JSON from response
            learnings = json.loads(response.content[0].text)
            return learnings

        except Exception as e:
            print(f"Error extracting learnings: {e}")
            return {
                "error": str(e),
                "questions_asked": [],
                "insights_discovered": [],
                "charts_generated": [],
                "agent_recommendations": [],
                "user_actions": []
            }

    def save_session_learnings(
        self,
        session_id: str,
        messages: list,
        user: str = "anonymous"
    ) -> bool:
        """
        Extract learnings and save to S3

        Args:
            session_id: Unique session identifier
            messages: List of conversation messages
            user: User who had the session

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Extract learnings using LLM
            learnings = self.extract_learnings_from_session(messages)

            # Create session record
            timestamp = datetime.now()
            session_data = {
                "session_id": session_id,
                "date": timestamp.strftime("%Y-%m-%d"),
                "time": timestamp.strftime("%H:%M:%S"),
                "user": user,
                "duration_minutes": None,  # Could calculate from timestamps
                "questions_asked": learnings.get("questions_asked", []),
                "insights_discovered": learnings.get("insights_discovered", []),
                "charts_generated": learnings.get("charts_generated", []),
                "agent_recommendations": learnings.get("agent_recommendations", []),
                "user_actions": learnings.get("user_actions", [])
            }

            # Save to S3: session_learnings/YYYY-MM-DD/session_{id}.json
            date_str = timestamp.strftime("%Y-%m-%d")
            file_key = f"{self.learnings_prefix}{date_str}/session_{session_id}.json"

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=json.dumps(session_data, indent=2)
            )

            return True

        except Exception as e:
            print(f"Error saving session learnings: {e}")
            return False

    def get_recent_learnings(self, days: int = 7) -> list:
        """
        Retrieve recent session learnings from S3

        Args:
            days: Number of days to look back

        Returns:
            list: List of session learning dictionaries
        """
        try:
            # List all learning files
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.learnings_prefix
            )

            if 'Contents' not in response:
                return []

            learnings_list = []

            # Get most recent files
            files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)

            for file_obj in files[:days * 10]:  # Assume ~10 sessions per day max
                file_key = file_obj['Key']

                # Skip directory markers
                if file_key.endswith('/'):
                    continue

                # Download and parse JSON
                obj_response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                content = obj_response['Body'].read().decode('utf-8')
                learnings_list.append(json.loads(content))

            return learnings_list

        except Exception as e:
            print(f"Error retrieving learnings: {e}")
            return []
