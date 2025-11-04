"""
Basin Climbing Analytics - Streamlit Web App

Main chat interface for the analytics agent.
"""

import streamlit as st
import sys
from pathlib import Path

# Add project root to path so we can import agent modules
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from agent.main_agent import run_agent
from utils.feedback_storage import FeedbackStorage
from utils.session_learnings import SessionLearningsStorage
from utils.conversation_logger import ConversationLogger
import uuid

# Page config
st.set_page_config(
    page_title="Cliff: Basin's Analytics Agent",
    page_icon="🧗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stChatMessage {
        padding: 1rem;
        border-radius: 0.5rem;
    }
    .stChatInput {
        border-radius: 0.5rem;
    }
    h1 {
        color: #1f77b4;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("🧗 Cliff: Basin's Analytics Agent")
st.markdown("Ask questions about your business data and get instant insights.")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.charts = []
    st.session_state.session_id = str(uuid.uuid4())[:8]  # Short session ID

# Initialize storage
feedback_storage = FeedbackStorage()
learnings_storage = SessionLearningsStorage()
conversation_logger = ConversationLogger()

# Sidebar
with st.sidebar:
    st.header("Feedback")
    st.markdown("Help us improve the analytics agent!")

    # Feedback form
    with st.form("feedback_form", clear_on_submit=True):
        feedback_type = st.selectbox(
            "Feedback Type",
            ["Bug Report", "Feature Request", "Agent Response Quality", "General Feedback"]
        )

        feedback_text = st.text_area(
            "Your feedback",
            placeholder="Tell us what you think...",
            height=100
        )

        user_name = st.text_input(
            "Your name (optional)",
            placeholder="e.g., Steel"
        )

        submitted = st.form_submit_button("Submit Feedback", type="primary")

        if submitted and feedback_text:
            # Save to S3
            success = feedback_storage.save_feedback(
                feedback_type=feedback_type,
                feedback_text=feedback_text,
                user=user_name if user_name else "anonymous"
            )

            if success:
                st.success("Thank you for your feedback!")
            else:
                st.error("Error saving feedback. Please try again.")
        elif submitted and not feedback_text:
            st.warning("Please enter some feedback before submitting.")

    st.divider()

    # Session controls
    st.header("Session")

    # Display session ID
    st.caption(f"Session ID: {st.session_state.session_id}")

    # End Session button (saves learnings)
    if st.button("End Session & Save Learnings", type="primary"):
        if len(st.session_state.messages) > 0:
            with st.spinner("Saving session data..."):
                # Save session learnings (summary)
                learnings_success = learnings_storage.save_session_learnings(
                    session_id=st.session_state.session_id,
                    messages=st.session_state.messages,
                    user="anonymous"  # TODO: Add user authentication
                )

                # Save full conversation log
                conversation_success = conversation_logger.log_full_session(
                    session_id=st.session_state.session_id,
                    conversation_history=st.session_state.messages,
                    metadata={
                        "turn_count": len([m for m in st.session_state.messages if m["role"] == "user"]),
                        "ended_by_user": True
                    },
                    user="streamlit_user"  # TODO: Add user authentication
                )

                if learnings_success and conversation_success:
                    st.success("Session saved successfully!")
                    # Clear and start new session
                    st.session_state.messages = []
                    st.session_state.session_id = str(uuid.uuid4())[:8]
                    st.rerun()
                elif learnings_success:
                    st.warning("Session summary saved, but full conversation logging failed. Clearing session anyway.")
                    st.session_state.messages = []
                    st.session_state.session_id = str(uuid.uuid4())[:8]
                    st.rerun()
                else:
                    st.error("Error saving session. Clearing anyway.")
                    st.session_state.messages = []
                    st.session_state.session_id = str(uuid.uuid4())[:8]
                    st.rerun()
        else:
            st.warning("No messages to save.")

    # Clear without saving
    if st.button("Clear Chat History", type="secondary"):
        st.session_state.messages = []
        st.session_state.session_id = str(uuid.uuid4())[:8]
        st.rerun()

    st.divider()

    # Info
    st.header("About")
    st.markdown("""
    **What can I ask?**
    - Revenue trends
    - Membership analytics
    - Instagram performance
    - Financial comparisons
    - Custom insights

    **Example questions:**
    - "What was revenue in October?"
    - "Show me membership growth"
    - "Which Instagram posts performed best?"
    """)

# Display chat history
for idx, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

        # Display chart if present
        if "chart_path" in message and message["chart_path"]:
            try:
                # Charts are saved as HTML files
                with open(message["chart_path"], 'r') as f:
                    chart_html = f.read()
                st.components.v1.html(chart_html, height=500, scrolling=True)
            except Exception as e:
                st.error(f"Error displaying chart: {e}")

# Chat input
if prompt := st.chat_input("Ask about Basin's analytics..."):
    # Add user message to chat
    st.session_state.messages.append({
        "role": "user",
        "content": prompt
    })

    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get agent response
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                # Run the agent
                response = run_agent(prompt)

                # Escape dollar signs to prevent LaTeX rendering issues
                response_display = response.replace('$', '\\$')

                # Display response
                st.markdown(response_display)

                # Check if a chart was generated
                # The agent saves charts to agent/charts/ directory
                # We'll detect the most recent chart
                import os
                from datetime import datetime

                chart_dir = project_root / "agent" / "charts"
                chart_path = None

                if chart_dir.exists():
                    # Get most recent HTML file
                    html_files = list(chart_dir.glob("*.html"))
                    if html_files:
                        # Sort by modification time, get most recent
                        latest_chart = max(html_files, key=os.path.getmtime)

                        # Check if it was created in the last few seconds (likely from this query)
                        if (datetime.now().timestamp() - os.path.getmtime(latest_chart)) < 10:
                            chart_path = str(latest_chart)

                            # Display the chart
                            with open(chart_path, 'r') as f:
                                chart_html = f.read()
                            st.components.v1.html(chart_html, height=500, scrolling=True)

                # Add assistant message to chat
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response,
                    "chart_path": chart_path
                })

                # Log the conversation turn to S3
                try:
                    metadata = {
                        "chart_generated": chart_path is not None,
                        "chart_path": str(chart_path) if chart_path else None,
                        "response_length": len(response)
                    }
                    conversation_logger.log_conversation_turn(
                        session_id=st.session_state.session_id,
                        user_question=prompt,
                        agent_response=response,
                        metadata=metadata,
                        user="streamlit_user"  # TODO: Add real user auth
                    )
                except Exception as log_error:
                    # Don't fail the whole interaction if logging fails
                    print(f"Warning: Failed to log conversation: {log_error}")

            except Exception as e:
                error_msg = f"Sorry, I encountered an error: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })

# Footer
st.divider()
st.caption("Cliff: Basin's Analytics Agent • Powered by Claude & Streamlit")
