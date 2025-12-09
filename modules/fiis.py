# modules/fiis.py
import os
import yfinance as yf
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
TICKERS_TABLE = "tickers_fiis"

class FIIProcessor:
    def __init__(self, engine):
        self.engine = engine

    def get_fii_data(self, ticker):
        ticker_yf = ticker if ticker.upper().endswith(".SA") else ticker + ".SA"
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
        with self.engine.begin() as conn:
            # Criar tabela se não existir
            conn.exec_driver_sql("""
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

            # Buscar tickers
            tickers = [r[0] for r in conn.execute(text(f"SELECT ticker FROM {TICKERS_TABLE}")).fetchall()]

            for i, ticker in enumerate(tickers, start=1):
                print(f"[{i}/{len(tickers)}] Buscando {ticker}...")
                data = self.get_fii_data(ticker)
                if not data:
                    continue

                # Inserir ou atualizar registro
                insert_sql = text("""
                    INSERT INTO historico_fiis (
                        data_registro, ticker, valor, dividend_yield, ultimo_rendimento,
                        p_vp, p_l, beta, patrimonio, liquidez_diaria, valor_em_caixa, setor, rentabilidade_12m
                    )
                    VALUES (
                        :data_registro, :ticker, :valor, :dividend_yield, :ultimo_rendimento,
                        :p_vp, :p_l, :beta, :patrimonio, :liquidez_diaria, :valor_em_caixa, :setor, :rentabilidade_12m
                    )
                    ON CONFLICT (ticker, data_registro) DO UPDATE SET
                        valor = EXCLUDED.valor,
                        dividend_yield = EXCLUDED.dividend_yield,
                        ultimo_rendimento = EXCLUDED.ultimo_rendimento,
                        p_vp = EXCLUDED.p_vp,
                        p_l = EXCLUDED.p_l,
                        beta = EXCLUDED.beta,
                        patrimonio = EXCLUDED.patrimonio,
                        liquidez_diaria = EXCLUDED.liquidez_diaria,
                        valor_em_caixa = EXCLUDED.valor_em_caixa,
                        setor = EXCLUDED.setor,
                        rentabilidade_12m = EXCLUDED.rentabilidade_12m
                """)

                params = {"data_registro": datetime.today().date(), "ticker": ticker.upper(), **data}
                conn.execute(insert_sql, params)
                print(f"✅ {ticker} inserido/atualizado com sucesso.")
