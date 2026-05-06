from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

@app.route("/")
def home():
    return "API Çalışıyor 🚀"

@app.route("/signals")
def get_signals():

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM signals ORDER BY time DESC LIMIT 300")
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(rows)

@app.route("/signals/latest")
def get_latest_signals():

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT s.*
        FROM signals s
        INNER JOIN (
            SELECT symbol, MAX(time) AS max_time
            FROM signals
            GROUP BY symbol
        ) son
        ON s.symbol = son.symbol AND s.time = son.max_time
        ORDER BY s.symbol ASC
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(rows)

@app.route("/signals/hourly/<symbol>")
def get_hourly_signal(symbol):

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT s.*
        FROM signals s
        INNER JOIN (
            SELECT HOUR(time) AS saat, MAX(id) AS max_id
            FROM signals
            WHERE symbol = %s
              AND DATE(time) = CURDATE()
              AND HOUR(time) BETWEEN 9 AND 18
            GROUP BY HOUR(time)
        ) son
        ON s.id = son.max_id
        WHERE s.symbol = %s
        ORDER BY s.time ASC
    """, (symbol, symbol))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(rows)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)