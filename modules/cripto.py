import requests
import psycopg2
from datetime import datetime, timezone



class CriptoFetcher:
    API_URL = "https://api.coingecko.com/api/v3/coins/markets"

    def __init__(self, per_page=100, total_pages=5):
        self.per_page = per_page
        self.total_pages = total_pages

    def fetch(self):
        """Busca dados de criptos no CoinGecko."""
        all_data = []

        for page in range(1, self.total_pages + 1):
            url = (
                f"{self.API_URL}"
                f"?vs_currency=usd&order=market_cap_desc"
                f"&per_page={self.per_page}&page={page}"
                f"&sparkline=false"
                f"&price_change_percentage=7d,30d,1y"
            )

            try:
                r = requests.get(url, timeout=10)
                r.raise_for_status()
                data = r.json()
                all_data.extend(data)
                print(f"Página {page} OK ({len(data)} moedas).")
            except Exception as e:
                print(f"❌ Erro ao buscar página {page}: {e}")

        return all_data


class CriptoSaver:
    def __init__(self, conn):
        self.conn = conn

    def save(self, cripto_list):
        """Insere lista de criptos no banco."""
        cursor = self.conn.cursor()
        tempo_utc = datetime.now(timezone.utc)

        sql = """
            INSERT INTO historico_cripto (
                tempo_utc, simbolo, nome, preco_atual, market_cap,
                market_cap_rank, fully_diluted_valuation, total_volume,
                high_24h, low_24h, price_change_24h, price_change_percentage_24h,
                market_cap_change_24h, market_cap_change_percentage_24h,
                circulating_supply, total_supply, max_supply, ath,
                ath_change_percentage, ath_date, atl, atl_change_percentage,
                atl_date, last_updated, price_change_percentage_1y_in_currency,
                price_change_percentage_30d_in_currency,
                price_change_percentage_7d_in_currency
            ) VALUES (
                %(tempo_utc)s, %(symbol)s, %(name)s, %(current_price)s, %(market_cap)s,
                %(market_cap_rank)s, %(fully_diluted_valuation)s, %(total_volume)s,
                %(high_24h)s, %(low_24h)s, %(price_change_24h)s, %(price_change_percentage_24h)s,
                %(market_cap_change_24h)s, %(market_cap_change_percentage_24h)s,
                %(circulating_supply)s, %(total_supply)s, %(max_supply)s, %(ath)s,
                %(ath_change_percentage)s, %(ath_date)s, %(atl)s, %(atl_change_percentage)s,
                %(atl_date)s, %(last_updated)s, %(price_change_percentage_1y_in_currency)s,
                %(price_change_percentage_30d_in_currency)s,
                %(price_change_percentage_7d_in_currency)s
            );
        """

        for item in cripto_list:
            item["tempo_utc"] = tempo_utc
            cursor.execute(sql, item)

        self.conn.commit()
        cursor.close()
        print(f"💾 {len(cripto_list)} criptos salvas no banco.")


class CriptoProcessor:
    def __init__(self, conn):
        self.conn = conn
        self.fetcher = CriptoFetcher()
        self.saver = CriptoSaver(conn)

    def run(self):
        print("\n🚀 Coletando dados de CRIPTO...")
        dados = self.fetcher.fetch()

        if not dados:
            print("❌ Nenhum dado encontrado para cripto.")
            return

        self.saver.save(dados)
        print("✔ Finalizado módulo CRIPTO.")
