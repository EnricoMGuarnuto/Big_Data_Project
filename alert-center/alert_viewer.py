from flask import Flask, render_template
import psycopg2

app = Flask(__name__, template_folder="templates")

DB_CONFIG = {
    "host": "postgres",
    "dbname": "retaildb",
    "user": "retail",
    "password": "retailpass"
}

@app.route("/")
def index():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT alert_id, item_id, location_id, rule_key, severity, created_at
        FROM alerts
        WHERE status = 'OPEN'
        ORDER BY created_at DESC
    """)
    alerts = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("index.html", alerts=alerts)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
