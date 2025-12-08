import requests
import yfinance as yf
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN")
TICKERS_TABLE = "tickers_bdr"

engine = create_engine(DATABASE_URL)

class BDRProcessor:
    def __init__(self, engine):
        self.engine = engine

    def fetch_brapi(self, ticker):
        try:
            url = f"https://brapi.dev/api/quote/{ticker}"
            r = requests.get(url, params={"token": BRAPI_TOKEN}, timeout=10).json()
            results = r.get("results")
            if not results:
                return None
            data = results[0]
            return {
                "preco_atual": data.get("regularMarketPrice"),
                "preco_52_semana_alta": data.get("fiftyTwoWeekHigh"),
                "preco_52_semana_baixa": data.get("fiftyTwoWeekLow"),
                "market_cap": data.get("marketCap"),
                "p_l": data.get("priceEarnings"),
            }
        except Exception:
            return None

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
            return None

    def merge_data(self, brapi_data, yahoo_data):
        return {**(brapi_data or {}), **(yahoo_data or {})}

    def run(self):
        # criação de tabela, leitura de tickers e inserção de dados
        # ... (mesmo código que você já tem)
        pass  # substitua pelo seu código de run
