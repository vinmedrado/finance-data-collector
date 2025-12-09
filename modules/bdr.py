# modules/bdr.py
import requests
import yfinance as yf
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
import os
import time

# -------------------------
# Configuração
# -------------------------
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN")
TICKERS_TABLE = "tickers_bdr"
SLEEP_BETWEEN = 0.3  # pausa entre chamadas

# -------------------------
# Conexão com DB
# -------------------------
engine = create_engine(DATABASE_URL)

# -------------------------
# Processor
# -------------------------
class BDRProcessor:
    def __init__(self, engine):
        self.engine = engine

    def fetch_brapi(self, ticker):
        try:
            url = f"https://brapi.dev/api/quote/{ticker}"
            r = requests.get(url, params={"token": BRAPI_TOKEN}, timeout=10).json()
            results = r.get("results")
            if not results:
                return {}
            data = results[0]
            return {
                "preco_atual": data.get("regularMarketPrice"),
                "preco_52_semana_alta": data.get("fiftyTwoWeekHigh"),
                "preco_52_semana_baixa": data.get("fiftyTwoWeekLow"),
                "market_cap": data.get("marketCap"),
                "p_l": data.get("priceEarnings"),
            }
        except Exception:
            return {}

    def fetch_yahoo(self, ticker):
        try:
            t = yf.Ticker(ticker + ".SA")
            info = t.info
            return {
                "preco_atual": info.get("regularMarketPrice"),
                "preco_52_semana_alta": info.get("fiftyTwoWeekHigh"),
                "preco_52_semana_baixa": info.get("fiftyTwoWeekLow"),
                "preco_media_50d": info.get("fiftyDayAverage"),
                "preco_media_200d": info.get("twoHundredDayAverage"),
                "p_l": info.get("trailingPE"),
                "p_vp": info.get("priceToBook"),
                "p_s": info.get("priceToSalesTrailing12Months"),
                "market_cap": info.get("marketCap"),
                "enterprise_value": info.get("enterpriseValue"),
                "roe": info.get("returnOnEquity"),
                "roa": info.get("returnOnAssets"),
                "margem_lucro": info.get("profitMargins"),
                "margem_operacional": info.get("operatingMargins"),
                "dividend_yield": info.get("dividendYield"),
                "payout_ratio": info.get("payoutRatio"),
                "crescimento_receita": info.get("revenueGrowth"),
                "crescimento_lucro": info.get("earningsGrowth"),
                "beta": info.get("beta"),
                "setor": info.get("sector"),
                "industria": info.get("industry"),
                "nome_empresa": info.get("longName"),
            }
        except Exception:
            return {}

    def merge_data(self, brapi_data, yahoo_data):
        return {**(brapi_data or {}), **(yahoo_data or {})}

    def run(self):
        fields = [
            "ticker", "preco_atual", "preco_52_semana_alta", "preco_52_semana_baixa",
            "preco_media_50d", "preco_media_200d", "p_l", "p_vp", "p_s", "market_cap",
            "enterprise_value", "roe", "roa", "margem_lucro", "margem_operacional",
            "dividend_yield", "payout_ratio", "crescimento_receita", "crescimento_lucro",
            "beta", "setor", "industria", "nome_empresa"
        ]

        # Criar tabela se não existir
        with self.engine.begin() as conn:
            conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS historico_bdr (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                preco_atual NUMERIC,
                preco_52_semana_alta NUMERIC,
                preco_52_semana_baixa NUMERIC,
                preco_media_50d NUMERIC,
                preco_media_200d NUMERIC,
                p_l NUMERIC,
                p_vp NUMERIC,
                p_s NUMERIC,
                market_cap NUMERIC,
                enterprise_value NUMERIC,
                roe NUMERIC,
                roa NUMERIC,
                margem_lucro NUMERIC,
                margem_operacional NUMERIC,
                dividend_yield NUMERIC,
                payout_ratio NUMERIC,
                crescimento_receita NUMERIC,
                crescimento_lucro NUMERIC,
                beta NUMERIC,
                setor TEXT,
                industria TEXT,
                nome_empresa TEXT,
                UNIQUE(ticker)
            )
            """)

        # Ler tickers
        with self.engine.begin() as conn:
            tickers = [r[0] for r in conn.execute(text(f"SELECT ticker FROM {TICKERS_TABLE}")).fetchall()]

        insert_sql = text("""
        INSERT INTO historico_bdr (
            ticker, preco_atual, preco_52_semana_alta, preco_52_semana_baixa,
            preco_media_50d, preco_media_200d, p_l, p_vp, p_s, market_cap,
            enterprise_value, roe, roa, margem_lucro, margem_operacional,
            dividend_yield, payout_ratio, crescimento_receita, crescimento_lucro,
            beta, setor, industria, nome_empresa
        ) VALUES (
            :ticker, :preco_atual, :preco_52_semana_alta, :preco_52_semana_baixa,
            :preco_media_50d, :preco_media_200d, :p_l, :p_vp, :p_s, :market_cap,
            :enterprise_value, :roe, :roa, :margem_lucro, :margem_operacional,
            :dividend_yield, :payout_ratio, :crescimento_receita, :crescimento_lucro,
            :beta, :setor, :industria, :nome_empresa
        )
        ON CONFLICT (ticker) DO UPDATE SET
            preco_atual = EXCLUDED.preco_atual,
            preco_52_semana_alta = EXCLUDED.preco_52_semana_alta,
            preco_52_semana_baixa = EXCLUDED.preco_52_semana_baixa,
            preco_media_50d = EXCLUDED.preco_media_50d,
            preco_media_200d = EXCLUDED.preco_media_200d,
            p_l = EXCLUDED.p_l,
            p_vp = EXCLUDED.p_vp,
            p_s = EXCLUDED.p_s,
            market_cap = EXCLUDED.market_cap,
            enterprise_value = EXCLUDED.enterprise_value,
            roe = EXCLUDED.roe,
            roa = EXCLUDED.roa,
            margem_lucro = EXCLUDED.margem_lucro,
            margem_operacional = EXCLUDED.margem_operacional,
            dividend_yield = EXCLUDED.dividend_yield,
            payout_ratio = EXCLUDED.payout_ratio,
            crescimento_receita = EXCLUDED.crescimento_receita,
            crescimento_lucro = EXCLUDED.crescimento_lucro,
            beta = EXCLUDED.beta,
            setor = EXCLUDED.setor,
            industria = EXCLUDED.industria,
            nome_empresa = EXCLUDED.nome_empresa
        """)

        # Loop de processamento
        for i, ticker in enumerate(tickers, start=1):
            if not ticker:
                print(f"⚠️ Ticker vazio na posição {i}, pulando...")
                continue

            print(f"[{i}/{len(tickers)}] Processando {ticker}...")
            try:
                brapi_data = self.fetch_brapi(ticker)
                yahoo_data = self.fetch_yahoo(ticker)

                merged = self.merge_data(brapi_data, yahoo_data)

                # Garantir que todos os campos existam para o SQL
                safe_data = {field: merged.get(field) for field in fields}
                safe_data["ticker"] = ticker  # garantir ticker preenchido

                with self.engine.begin() as conn:
                    conn.execute(insert_sql, safe_data)

                print(f"✅ {ticker} atualizado com sucesso.")
            except Exception as e:
                print(f"❌ Erro {ticker}: {e}")

            time.sleep(SLEEP_BETWEEN)

        print("=== Processamento de BDR finalizado ===")
