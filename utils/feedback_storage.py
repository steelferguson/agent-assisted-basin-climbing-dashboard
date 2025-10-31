"""
Feedback Storage - Collect and store team feedback in S3

Stores feedback in JSONL format (JSON Lines - one JSON object per line)
"""

import boto3
import json
from datetime import datetime
from data_pipeline import config


class FeedbackStorage:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        self.bucket_name = config.aws_bucket_name
        self.feedback_prefix = "agent_feedback/"

    def save_feedback(
        self,
        feedback_type: str,
        feedback_text: str,
        user: str = "anonymous"
    ) -> bool:
        """
        Save feedback to S3 in JSONL format

        Args:
            feedback_type: Type of feedback (Bug Report, Feature Request, etc.)
            feedback_text: The feedback content
            user: User who submitted feedback (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            timestamp = datetime.now()
            date_str = timestamp.strftime("%Y-%m-%d")

            # Create feedback entry
            feedback_entry = {
                "timestamp": timestamp.isoformat(),
                "user": user,
                "type": feedback_type,
                "text": feedback_text
            }

            # File path: feedback/YYYY-MM-DD_feedback.jsonl
            file_key = f"{self.feedback_prefix}{date_str}_feedback.jsonl"

            # Try to get existing file
            try:
                response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                existing_content = response['Body'].read().decode('utf-8')
            except self.s3.exceptions.NoSuchKey:
                existing_content = ""

            # Append new feedback (JSONL format - one JSON per line)
            new_line = json.dumps(feedback_entry) + "\n"
            updated_content = existing_content + new_line

            # Upload back to S3
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=file_key,
                Body=updated_content
            )

            return True

        except Exception as e:
            print(f"Error saving feedback: {e}")
            return False

    def get_recent_feedback(self, days: int = 7) -> list:
        """
        Retrieve recent feedback from S3

        Args:
            days: Number of days to look back

        Returns:
            list: List of feedback dictionaries
        """
        try:
            # List all feedback files
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.feedback_prefix
            )

            if 'Contents' not in response:
                return []

            feedback_list = []

            # Get most recent files (sorted by date in filename)
            files = sorted(response['Contents'], key=lambda x: x['Key'], reverse=True)

            for file_obj in files[:days]:  # Get last N days
                file_key = file_obj['Key']

                # Download and parse JSONL
                obj_response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                content = obj_response['Body'].read().decode('utf-8')

                # Parse each line as JSON
                for line in content.strip().split('\n'):
                    if line:
                        feedback_list.append(json.loads(line))

            return feedback_list

        except Exception as e:
            print(f"Error retrieving feedback: {e}")
            return []
