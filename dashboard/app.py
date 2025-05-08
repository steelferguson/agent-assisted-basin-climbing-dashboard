import sys
sys.path.append('./src')
from dash import Dash
from dashboard import create_dashboard
import os

# Initialize the Dash app
app = Dash(__name__)

# Create the dashboard layout and callbacks
create_dashboard(app)

# Expose the Flask server for Gunicorn
server = app.server

if __name__ == "__main__":
    app.run(debug=True)