import os
import time
import threading
from flask import Flask

app = Flask(__name__)

def background_scanner():
    while True:
        print("Running automated weather arbitrage scan across all cities...")
        try:
            # --- Your weather check and Kalshi execution logic runs here ---
            pass
        except Exception as e:
            print(f"Error in background scanner: {e}")
        
        # Wait 1 hour (3600 seconds) before scanning again
        time.sleep(3600)

@app.route("/")
def home():
    return "Weather bot is active and scanning in the background!"

if __name__ == "__main__":
    # Start the background scanning loop in a separate thread so Flask can stay online
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
