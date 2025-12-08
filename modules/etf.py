# modules/etf.py
import os
import psycopg2
import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ.get("DATABASE_URL")  # pega a URL do banco
BRAPI_KEY = os.getenv("BRAPI_TOKEN")  # token BRAPI
BRAPI_URL = "https://brapi.dev/api/quote/"

class ETFDataFetcher:
    def is_brazil_etf(self, ticker):
        return ticker.upper().endswith("11")

    def fetch_brapi(self, ticker):
        if not BRAPI_KEY:
            return {}
        try:
            url = f"{BRAPI_URL}{ticker}?token={BRAPI_KEY}"
            r = requests.get(url, timeout=10).json()
            if "results" in r and r["results"]:
                return r["results"][0]
        except Exception as e:
            print("Erro BRAPI ETF:", e)
        return {}

    def fetch_yahoo(self, ticker):
        try:
            return yf.Ticker(ticker).info or {}
        except Exception as e:
            print("Erro Yahoo ETF:", e)
            return {}

    def get_etf_data(self, ticker):
        is_br = self.is_brazil_etf(ticker)
        yf_ticker = ticker + ".SA" if is_br else ticker
        print(f"\n🔎 Buscando: {yf_ticker} (BR={is_br})")
        yahoo = self.fetch_yahoo(yf_ticker)
        brapi = self.fetch_brapi(ticker) if is_br else {}
        data = {}
        data["preco_atual"] = yahoo.get("currentPrice") or yahoo.get("regularMarketPrice") or brapi.get("regularMarketPrice")
        if not data["preco_atual"]:
            print(f"⚠️ Sem preço para {ticker}, pulando.")
            return None
        data["variacao_dia"] = yahoo.get("regularMarketChangePercent")
        data["variacao_12m"] = yahoo.get("52WeekChange")
        data["variacao_1m"] = None
        data["variacao_6m"] = None
        data["fifty_two_week_low"] = yahoo.get("fiftyTwoWeekLow") or brapi.get("fiftyTwoWeekLow")
        data["fifty_two_week_high"] = yahoo.get("fiftyTwoWeekHigh") or brapi.get("fiftyTwoWeekHigh")
        data["p_l"] = brapi.get("priceEarnings") or yahoo.get("trailingPE")
        data["p_vp"] = yahoo.get("priceToBook")
        data["dividend_yield"] = yahoo.get("dividendYield") or brapi.get("dividendYield")
        data["beta"] = yahoo.get("beta")
        data["volume"] = yahoo.get("volume") or brapi.get("regularMarketVolume")
        data["market_cap"] = yahoo.get("marketCap") or brapi.get("marketCap")
        data["setor"] = yahoo.get("category") or yahoo.get("industry")
        data["pais"] = "BR" if is_br else "US"
        return data

class ETFDatabaseSaver:
    def save(self, conn, ticker, data):
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS historico_etf (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT,
                    preco_atual NUMERIC,
                    variacao_dia NUMERIC,
                    variacao_1m NUMERIC,
                    variacao_6m NUMERIC,
                    variacao_12m NUMERIC,
                    fifty_two_week_low NUMERIC,
                    fifty_two_week_high NUMERIC,
                    p_l NUMERIC,
                    p_vp NUMERIC,
                    dividend_yield NUMERIC,
                    beta NUMERIC,
                    volume NUMERIC,
                    market_cap NUMERIC,
                    setor TEXT,
                    pais TEXT,
                    data_registro DATE DEFAULT CURRENT_DATE
                );
            """)
            cursor.execute("""
                INSERT INTO historico_etf (
                    ticker, preco_atual, variacao_dia, variacao_1m, variacao_6m, variacao_12m,
                    fifty_two_week_low, fifty_two_week_high, p_l, p_vp, dividend_yield,
                    beta, volume, market_cap, setor, pais
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                ticker,
                data.get("preco_atual"),
                data.get("variacao_dia"),
                data.get("variacao_1m"),
                data.get("variacao_6m"),
                data.get("variacao_12m"),
                data.get("fifty_two_week_low"),
                data.get("fifty_two_week_high"),
                data.get("p_l"),
                data.get("p_vp"),
                data.get("dividend_yield"),
                data.get("beta"),
                data.get("volume"),
                data.get("market_cap"),
                data.get("setor"),
                data.get("pais"),
            ))
            conn.commit()
            print(f"💾 Salvo: {ticker}")

class ETFProcessor:
    def __init__(self):
        self.fetcher = ETFDataFetcher()
        self.saver = ETFDatabaseSaver()
        # conecta ao banco ao criar instância
        self.conn = psycopg2.connect(DB_URL)

    def run(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT ticker FROM tickers_etf;")
            tickers = [r[0] for r in cur.fetchall()]
            for ticker in tickers:
                try:
                    data = self.fetcher.get_etf_data(ticker)
                    if data:
                        self.saver.save(self.conn, ticker, data)
                except Exception as e:
                    print(f"❌ Erro com {ticker}: {e}")
        self.conn.close()

def processar_etf():
    ETFProcessor().run()
