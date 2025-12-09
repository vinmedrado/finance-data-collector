from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine

# Carrega .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

try:
    # Teste com psycopg2
    conn_test = psycopg2.connect(DATABASE_URL)
    cur = conn_test.cursor()
    cur.execute("SELECT NOW();")
    print(f"✅ Conectado ao Neon! Hora do servidor: {cur.fetchone()[0]}")
    cur.close()
    conn_test.close()
except Exception as e:
    print(f"❌ Erro ao conectar no Neon: {e}")

# Cria engine para módulos que usam SQLAlchemy
engine = create_engine(DATABASE_URL)

# Importa módulos
from modules.fiis import FIIProcessor
from modules.etf import ETFProcessor, ETFDataFetcher, ETFDatabaseSaver
from modules.acoes import AcoesProcessor
from modules.bdr import BDRProcessor
from modules.cripto import CriptoProcessor
import psycopg2

print("\n==============================")
print("  📊 COLETOR FINANCEIRO INICIADO")
print("==============================\n")

# ----------------------------
# FIIs
# ----------------------------
try:
    fii_proc = FIIProcessor()
    fii_proc.run()
except Exception as e:
    print(f"❌ ERRO durante o processamento de FIIs: {e}")

# ----------------------------
# Ações
# ----------------------------
try:
    acoes_proc = AcoesProcessor(engine)
    acoes_proc.run()
except Exception as e:
    print(f"❌ ERRO durante o processamento de Ações: {e}")

# ----------------------------
# BDRs
# ----------------------------
try:
    bdr_proc = BDRProcessor(engine)
    bdr_proc.run()
except Exception as e:
    print(f"❌ ERRO durante o processamento de BDR: {e}")

# ----------------------------
# Criptos
# ----------------------------
try:
    # Cripto usa psycopg2 direto
    conn = psycopg2.connect(DATABASE_URL)
    cripto_proc = CriptoProcessor(conn)
    cripto_proc.run()
    conn.close()
except Exception as e:
    print(f"❌ ERRO durante o processamento de Criptos: {e}")

# ----------------------------
# ETFs
# ----------------------------
try:
    etf_proc = ETFProcessor()
    etf_proc.run()
except Exception as e:
    print(f"❌ ERRO durante o processamento de ETF: {e}")

print("\n==============================")
print("  ✅ COLETA FINALIZADA")
print("==============================\n")
