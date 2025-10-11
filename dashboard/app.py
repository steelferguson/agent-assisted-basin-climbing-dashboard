import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dash import Dash
from dashboard.dashboard import create_dashboard

# Initialize the Dash app
app = Dash(__name__)

# Create the dashboard layout and callbacks
create_dashboard(app)

# Expose the Flask server for Gunicorn
server = app.server

if __name__ == "__main__":
    app.run(debug=True)
