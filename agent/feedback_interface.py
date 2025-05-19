import json


def capture_feedback(insights):
    """
    Local test version of feedback capture.
    Accepts feedback for each insight from the console.
    In production, this would be replaced by a form or web interface.
    Returns a list of feedback entries.
    """
    feedback_entries = []
    print(
        "\nPlease provide feedback for each insight. Type 'skip' to skip any insight.\n"
    )

    for insight in insights.split("\n"):
        if not insight.strip():
            continue
        print(f"Insight: {insight}")
        user = input("Your name: ").strip()
        if not user:
            user = "anonymous"
        comment = input("Feedback / Explanation / Hypothesis: ").strip()
        if comment.lower() == "skip":
            continue
        feedback_entries.append({"user": user, "insight": insight, "comment": comment})
        print("Recorded.\n")

    return feedback_entries


def save_feedback_to_file(feedback_entries, filepath="agent/feedback_log.json"):
    try:
        with open(filepath, "a") as f:
            for entry in feedback_entries:
                f.write(json.dumps(entry) + "\n")
    except Exception as e:
        print(f"Error saving feedback: {e}")
