# modules/fiis.py
import os
import yfinance as yf
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ.get("DATABASE_URL")  # pega a URL do banco do .env

class FIIProcessor:
    def __init__(self):
        # Conecta ao banco ao criar a instância
        self.conn = psycopg2.connect(DB_URL)

    def get_fii_data(self, ticker):
        ticker_yf = ticker + ".SA"
        try:
            t = yf.Ticker(ticker_yf)
            info = t.info
            return {
                "valor": info.get("regularMarketPrice"),
                "dividend_yield": info.get("dividendYield"),
                "ultimo_rendimento": info.get("lastDividendValue"),
                "p_vp": info.get("priceToBook"),
                "p_l": info.get("trailingPE"),
                "beta": info.get("beta"),
                "patrimonio": info.get("totalAssets"),
                "liquidez_diaria": info.get("averageDailyVolume10Day"),
                "valor_em_caixa": info.get("cash"),
                "setor": info.get("sector"),
                "rentabilidade_12m": info.get("52WeekChange"),
            }
        except Exception as e:
            print(f"❌ Erro ao buscar FII {ticker}: {e}")
            return None

    def run(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS historico_fiis (
                    id SERIAL PRIMARY KEY,
                    data_registro DATE,
                    ticker TEXT,
                    valor NUMERIC,
                    dividend_yield NUMERIC,
                    ultimo_rendimento NUMERIC,
                    p_vp NUMERIC,
                    p_l NUMERIC,
                    beta NUMERIC,
                    patrimonio NUMERIC,
                    liquidez_diaria NUMERIC,
                    valor_em_caixa NUMERIC,
                    setor TEXT,
                    rentabilidade_12m NUMERIC,
                    UNIQUE(ticker, data_registro)
                )
            """)
            self.conn.commit()

            cur.execute("SELECT ticker FROM tickers_fiis")
            tickers = [r[0] for r in cur.fetchall()]

            for i, ticker in enumerate(tickers, start=1):
                print(f"[{i}/{len(tickers)}] Buscando {ticker}...")
                data = self.get_fii_data(ticker)
                if not data:
                    continue

                cur.execute("""
                    SELECT 1 FROM historico_fiis WHERE ticker=%s AND data_registro=%s
                """, (ticker.upper(), datetime.today().date()))
                if cur.fetchone():
                    print(f"⚠️ {ticker} já registrado hoje, pulando...")
                    continue

                try:
                    cur.execute("""
                        INSERT INTO historico_fiis
                        (data_registro, ticker, valor, dividend_yield, ultimo_rendimento, p_vp, p_l,
                         beta, patrimonio, liquidez_diaria, valor_em_caixa, setor, rentabilidade_12m)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        datetime.today(),
                        ticker.upper(),
                        data["valor"],
                        data["dividend_yield"],
                        data["ultimo_rendimento"],
                        data["p_vp"],
                        data["p_l"],
                        data["beta"],
                        data["patrimonio"],
                        data["liquidez_diaria"],
                        data["valor_em_caixa"],
                        data["setor"],
                        data["rentabilidade_12m"]
                    ))
                    self.conn.commit()
                    print(f"✅ {ticker} inserido com sucesso.")
                except Exception as e:
                    print(f"❌ Erro ao inserir {ticker}: {e}")
                    self.conn.rollback()
