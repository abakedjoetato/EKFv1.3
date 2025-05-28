"""
Railway Keep-Alive HTTP Server
Provides a simple HTTP endpoint for Railway deployment health checks
"""

import os
from threading import Thread
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return {
        "status": "online",
        "service": "Emerald's Killfeed Discord Bot",
        "version": "2.0.0"
    }

@app.route('/health')
def health():
    return {"status": "healthy", "bot": "running"}

def run():
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

def keep_alive():
    t = Thread(target=run)
    t.daemon = True
    t.start()