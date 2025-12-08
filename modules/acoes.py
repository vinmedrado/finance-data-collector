# modules/acoes.py
import time
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os

# -------------------------
# CONFIGURAÇÃO
# -------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")  # Pegará do .env
SLEEP_BETWEEN = 0.3  # pausa entre chamadas
TICKERS_TABLE = "tickers_acoes"

# -------------------------
# Conexão com DB
# -------------------------
engine = create_engine(DATABASE_URL)

# -------------------------
# Processor
# -------------------------
class AcoesProcessor:
    def __init__(self, engine):
        self.engine = engine

    def fetch_data(self, ticker):
        """Busca dados do Yahoo Finance para o ticker"""
        ticker_yf = ticker if ticker.upper().endswith(".SA") else ticker + ".SA"
        yf_ticker = yf.Ticker(ticker_yf)

        # Histórico diário
        hist = yf_ticker.history(period="1d")
        if hist.empty:
            return None

        row = hist.iloc[-1]
        info = yf_ticker.info

        data = {
            "ticker": ticker,
            "data": row.name.date(),
            "preco_abertura": float(row.get("Open") or 0),
            "preco_fechamento": float(row.get("Close") or 0),
            "preco_maximo": float(row.get("High") or 0),
            "preco_minimo": float(row.get("Low") or 0),
            "volume": float(row.get("Volume") or 0),
            "pl": float(info.get("trailingPE") or 0),
            "pvp": float(info.get("priceToBook") or 0),
            "beta": float(info.get("beta") or 0),
            "dividend_yield": float(info.get("dividendYield") or 0),
            "last_dividend": float(info.get("lastDividendValue") or 0),
            "dividend_date": datetime.fromtimestamp(info.get("lastDividendDate")).date() if info.get("lastDividendDate") else None
        }
        return data

    def run(self):
        """Processa todos os tickers da tabela"""
        # Criar tabela se não existir
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

        # Ler tickers
        tickers_df = pd.read_sql(f"SELECT ticker FROM {TICKERS_TABLE} ORDER BY id ASC", self.engine)
        tickers = tickers_df['ticker'].tolist()

        insert_sql = text("""
        INSERT INTO historico_acoes (
            ticker, data, preco_abertura, preco_fechamento, preco_maximo, preco_minimo,
            volume, pl, pvp, beta, dividend_yield, last_dividend, dividend_date
        )
        VALUES (
            :ticker, :data, :preco_abertura, :preco_fechamento, :preco_maximo, :preco_minimo,
            :volume, :pl, :pvp, :beta, :dividend_yield, :last_dividend, :dividend_date
        )
        ON CONFLICT (ticker, data) DO UPDATE
        SET
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

        # Loop de processamento
        for i, ticker in enumerate(tickers, start=1):
            print(f"[{i}/{len(tickers)}] Processando {ticker}...")
            try:
                data = self.fetch_data(ticker)
                if not data:
                    print(f"⚠️ Nenhum dado para {ticker}")
                    continue

                with self.engine.begin() as conn:
                    conn.execute(insert_sql, data)

                print(f"✅ {ticker} atualizado com sucesso.")
            except Exception as e:
                print(f"❌ Erro {ticker}: {e}")

            time.sleep(SLEEP_BETWEEN)

        print("=== Processamento de ações finalizado ===")
