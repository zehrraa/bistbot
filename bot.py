import mysql.connector
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# veritabanı bağlantısı
conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)

cursor = conn.cursor()

# tablo oluşturma
cursor.execute("""
CREATE TABLE IF NOT EXISTS signals(
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20),
    time DATETIME,
    close FLOAT,
    ema200 FLOAT,
    rsi FLOAT
)
""")

conn.commit()


def bist30_listesi():

    url = "https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/Temel-Degerler-Ve-Oranlar.aspx?endeks=03#page-1"

    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    tablo = soup.find("table", {"id": "summaryBasicData"})
    tablo = pd.read_html(StringIO(str(tablo)))[0]

    tablo.columns = tablo.columns.str.strip()

    hisseler = []

    for kod in tablo["Kod"]:
        hisseler.append(kod + ".IS")

    return hisseler


def calistir():

    bist30 = bist30_listesi()

    data = []
    ema_up = []
    ema_down = []
    ema_durum = {}
    momentum = []

    emaUst_rsi80 = []
    emaUst_rsi30 = []

    emaAlt_rsi80 = []
    emaAlt_rsi30 = []

    for stock in bist30:

        try:

            ticker = yf.Ticker(stock)

            hist = ticker.history(period="1y", interval="4h")

            if len(hist) < 200:
                continue

            hist["EMA200"] = hist["Close"].ewm(span=200, adjust=False).mean()

            delta = hist["Close"].diff()

            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)

            avg_gain = gain.rolling(14).mean()
            avg_loss = loss.rolling(14).mean()

            rs = avg_gain / avg_loss

            hist["RSI"] = 100 - (100 / (1 + rs))

            today_close = hist["Close"].iloc[-1]
            yesterday_close = hist["Close"].iloc[-2]

            today_ema = hist["EMA200"].iloc[-1]
            yesterday_ema = hist["EMA200"].iloc[-2]

            today_rsi = hist["RSI"].iloc[-1]

            # 🔴 VERİTABANINA KAYDET
            cursor.execute("""
            INSERT INTO signals(symbol,time,close,ema200,rsi)
            VALUES(%s,%s,%s,%s,%s)
            """,(
                stock,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                float(today_close),
                float(today_ema),
                float(today_rsi)
            ))

            conn.commit()

            change = (today_close - yesterday_close) / yesterday_close * 100

            data.append([stock, change])

            # EMA durumu
            if today_close > today_ema:

                ema_durum[stock] = "EMA200 üstünde ⬆️"

                if today_rsi > 80:
                    emaUst_rsi80.append([stock, today_rsi])

                if today_rsi < 30:
                    emaUst_rsi30.append([stock, today_rsi])

            else:

                ema_durum[stock] = "EMA200 altında ⬇️"

                if today_rsi > 80:
                    emaAlt_rsi80.append([stock, today_rsi])

                if today_rsi < 30:
                    emaAlt_rsi30.append([stock, today_rsi])

            if today_close > today_ema:
                ema_up.append(stock)

            if today_close < today_ema:
                ema_down.append(stock)

            if change > 3 and today_close > today_ema:
                momentum.append([stock, change])

        except Exception as e:
            print(stock, "hata:", e)

    def telegram_gonder(text):

        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

        requests.post(url, data={
            "chat_id": CHAT_ID,
            "text": text
        })

    msg1 = "📈 EMA200 ÜSTÜ + RSI > 80\n\n"

    if emaUst_rsi80:
        for stock, rsi in emaUst_rsi80:
            msg1 += f"{stock} RSI:{rsi:.1f}\n"
    else:
        msg1 += "Yok"

    telegram_gonder(msg1)

    msg2 = "📉 EMA200 ÜSTÜ + RSI < 30\n\n"

    if emaUst_rsi30:
        for stock, rsi in emaUst_rsi30:
            msg2 += f"{stock} RSI:{rsi:.1f}\n"
    else:
        msg2 += "Yok"

    telegram_gonder(msg2)

    msg3 = "📈 EMA200 ALTI + RSI > 80\n\n"

    if emaAlt_rsi80:
        for stock, rsi in emaAlt_rsi80:
            msg3 += f"{stock} RSI:{rsi:.1f}\n"
    else:
        msg3 += "Yok"

    telegram_gonder(msg3)

    msg4 = "📉 EMA200 ALTI + RSI < 30\n\n"

    if emaAlt_rsi30:
        for stock, rsi in emaAlt_rsi30:
            msg4 += f"{stock} RSI:{rsi:.1f}\n"
    else:
        msg4 += "Yok"

    telegram_gonder(msg4)

    print("4 RSI mesajı gönderildi.")


calistir()

# veritabanındaki kayıtları göster
cursor.execute("SELECT * FROM signals")

rows = cursor.fetchall()

print("\nVERİTABANI KAYITLARI:\n")

for row in rows:
    print(row)