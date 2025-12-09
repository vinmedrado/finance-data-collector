# modules/etf.py
import os
import yfinance as yf
import requests
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import time

load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")
BRAPI_TOKEN = os.environ.get("BRAPI_TOKEN")
BRAPI_URL = "https://brapi.dev/api/quote/"
engine = create_engine(DATABASE_URL)
TICKERS_TABLE = "tickers_etf"
SLEEP_BETWEEN = 0.3

class ETFProcessor:
    def __init__(self, engine):
        self.engine = engine

    def is_brazil_etf(self, ticker):
        return ticker.upper().endswith("11")

    def fetch_brapi(self, ticker):
        if not BRAPI_TOKEN:
            return {}
        try:
            url = f"{BRAPI_URL}{ticker}?token={BRAPI_TOKEN}"
            r = requests.get(url, timeout=10).json()
            return r.get("results", [{}])[0]
        except Exception as e:
            print(f"Erro BRAPI ETF {ticker}:", e)
            return {}

    def fetch_yahoo(self, ticker):
        try:
            return yf.Ticker(ticker).info or {}
        except Exception as e:
            print(f"Erro Yahoo ETF {ticker}:", e)
            return {}

    def get_data(self, ticker):
        is_br = self.is_brazil_etf(ticker)
        yf_ticker = ticker + ".SA" if is_br else ticker

        yahoo = self.fetch_yahoo(yf_ticker)
        brapi = self.fetch_brapi(ticker) if is_br else {}

        preco_atual = (
            yahoo.get("currentPrice")
            or yahoo.get("regularMarketPrice")
            or brapi.get("regularMarketPrice")
        )
        if not preco_atual:
            return None

        return {
            "ticker": ticker.upper(),
            "preco_atual": preco_atual,
            "variacao_dia": yahoo.get("regularMarketChangePercent"),
            "variacao_12m": yahoo.get("52WeekChange"),
            "variacao_1m": None,
            "variacao_6m": None,
            "fifty_two_week_low": yahoo.get("fiftyTwoWeekLow") or brapi.get("fiftyTwoWeekLow"),
            "fifty_two_week_high": yahoo.get("fiftyTwoWeekHigh") or brapi.get("fiftyTwoWeekHigh"),
            "p_l": brapi.get("priceEarnings") or yahoo.get("trailingPE"),
            "p_vp": yahoo.get("priceToBook"),
            "dividend_yield": yahoo.get("dividendYield") or brapi.get("dividendYield"),
            "beta": yahoo.get("beta"),
            "volume": yahoo.get("volume") or brapi.get("regularMarketVolume"),
            "market_cap": yahoo.get("marketCap") or brapi.get("marketCap"),
            "setor": yahoo.get("category") or yahoo.get("industry"),
            "pais": "BR" if is_br else "US",
            "data_registro": datetime.now().date()
        }

    def run(self):
        # Criar tabela sem UNIQUE e sem conflito
        with self.engine.begin() as conn:
            conn.exec_driver_sql("""
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
                data_registro DATE
            )
            """)

        # Buscar tickers
        with self.engine.begin() as conn:
            tickers = [
                r[0] for r in conn.execute(text(f"SELECT ticker FROM {TICKERS_TABLE}")).fetchall()
            ]

        # SQL simples (insere sempre)
        insert_sql = text("""
        INSERT INTO historico_etf (
            ticker, preco_atual, variacao_dia, variacao_1m, variacao_6m, variacao_12m,
            fifty_two_week_low, fifty_two_week_high, p_l, p_vp, dividend_yield,
            beta, volume, market_cap, setor, pais, data_registro
        )
        VALUES (
            :ticker, :preco_atual, :variacao_dia, :variacao_1m, :variacao_6m, :variacao_12m,
            :fifty_two_week_low, :fifty_two_week_high, :p_l, :p_vp, :dividend_yield,
            :beta, :volume, :market_cap, :setor, :pais, :data_registro
        )
        """)

        fields = [
            "ticker", "preco_atual", "variacao_dia", "variacao_1m", "variacao_6m",
            "variacao_12m", "fifty_two_week_low", "fifty_two_week_high",
            "p_l", "p_vp", "dividend_yield", "beta", "volume", "market_cap",
            "setor", "pais", "data_registro"
        ]

        # Loop de processamento
        for i, ticker in enumerate(tickers, start=1):
            print(f"[{i}/{len(tickers)}] Processando {ticker}...")

            data = self.get_data(ticker)
            if not data:
                print(f"⚠️ Sem dados para {ticker}")
                continue

            safe_data = {f: data.get(f) for f in fields}

            with self.engine.begin() as conn:
                conn.execute(insert_sql, safe_data)

            print(f"✅ {ticker} registrado.")
            time.sleep(SLEEP_BETWEEN)

        print("=== Processamento de ETFs finalizado ===")
