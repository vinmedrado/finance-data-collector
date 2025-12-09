# modules/acoes.py
import os
import yfinance as yf
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import time

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
TICKERS_TABLE = "tickers_acoes"
SLEEP_BETWEEN = 0.3  # pausa entre chamadas

class AcoesProcessor:
    def __init__(self, engine):
        self.engine = engine

    def fetch_data(self, ticker):
        ticker_yf = ticker if ticker.upper().endswith(".SA") else ticker + ".SA"
        t = yf.Ticker(ticker_yf)

        hist = t.history(period="1d")
        if hist.empty:
            return None

        row = hist.iloc[-1]
        info = t.info or {}

        data = {
            "ticker": ticker.upper(),
            "data": row.name.date(),
            "preco_abertura": row.get("Open"),
            "preco_fechamento": row.get("Close"),
            "preco_maximo": row.get("High"),
            "preco_minimo": row.get("Low"),
            "volume": row.get("Volume"),
            "pl": info.get("trailingPE"),
            "pvp": info.get("priceToBook"),
            "beta": info.get("beta"),
            "dividend_yield": info.get("dividendYield"),
            "last_dividend": info.get("lastDividendValue"),
            "dividend_date": datetime.fromtimestamp(info.get("lastDividendDate")).date() if info.get("lastDividendDate") else None
        }
        return data

    def run(self):
        with self.engine.begin() as conn:
            conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS historico_acoes (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                data DATE NOT NULL,
                preco_abertura NUMERIC,
                preco_fechamento NUMERIC,
                preco_maximo NUMERIC,
                preco_minimo NUMERIC,
                volume NUMERIC,
                pl NUMERIC,
                pvp NUMERIC,
                beta NUMERIC,
                dividend_yield NUMERIC,
                last_dividend NUMERIC,
                dividend_date DATE,
                UNIQUE(ticker, data)
            )
            """)

            tickers = [r[0] for r in conn.execute(text(f"SELECT ticker FROM {TICKERS_TABLE}")).fetchall()]

            insert_sql = text("""
            INSERT INTO historico_acoes (
                ticker, data, preco_abertura, preco_fechamento, preco_maximo, preco_minimo,
                volume, pl, pvp, beta, dividend_yield, last_dividend, dividend_date
            )
            VALUES (
                :ticker, :data, :preco_abertura, :preco_fechamento, :preco_maximo, :preco_minimo,
                :volume, :pl, :pvp, :beta, :dividend_yield, :last_dividend, :dividend_date
            )
            ON CONFLICT (ticker, data) DO UPDATE SET
                preco_abertura = EXCLUDED.preco_abertura,
                preco_fechamento = EXCLUDED.preco_fechamento,
                preco_maximo = EXCLUDED.preco_maximo,
                preco_minimo = EXCLUDED.preco_minimo,
                volume = EXCLUDED.volume,
                pl = EXCLUDED.pl,
                pvp = EXCLUDED.pvp,
                beta = EXCLUDED.beta,
                dividend_yield = EXCLUDED.dividend_yield,
                last_dividend = EXCLUDED.last_dividend,
                dividend_date = EXCLUDED.dividend_date
            """)

            for i, ticker in enumerate(tickers, start=1):
                if not ticker:
                    print(f"⚠️ Ticker vazio na posição {i}, pulando...")
                    continue

                print(f"[{i}/{len(tickers)}] Processando {ticker}...")
                try:
                    data = self.fetch_data(ticker)
                    if not data:
                        print(f"⚠️ Nenhum dado para {ticker}")
                        continue

                    # Garantir campos existentes mesmo que None
                    safe_data = {k: data.get(k) for k in [
                        "ticker", "data", "preco_abertura", "preco_fechamento", "preco_maximo", "preco_minimo",
                        "volume", "pl", "pvp", "beta", "dividend_yield", "last_dividend", "dividend_date"
                    ]}

                    with self.engine.begin() as conn:
                        conn.execute(insert_sql, safe_data)

                    print(f"✅ {ticker} atualizado com sucesso.")
                except Exception as e:
                    print(f"❌ Erro {ticker}: {e}")

                time.sleep(SLEEP_BETWEEN)

        print("=== Processamento de ações finalizado ===")
