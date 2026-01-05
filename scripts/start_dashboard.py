import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from analytics.dashboard.app import app

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

