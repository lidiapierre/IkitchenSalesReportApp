import time, requests

APP_URL = "https://ikitchen-daily-sales-report.streamlit.app"

# To start the heartbeat script: `nohup python heartbeat.py & echo $! > heartbeat.pid`
# To kill it: `kill $(cat heartbeat.pid)`


while True:
    try:
        r = requests.get(APP_URL)
        print("Pinged app, status:", r.status_code)
    except Exception as e:
        print("Ping error:", e)
    time.sleep(10 * 60 * 60)  # every 10 hours
