from dotenv import load_dotenv
import os
import psycopg2
from modules.cripto import CriptoProcessor

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
cripto_proc = CriptoProcessor(conn)
cripto_proc.run()
conn.close()
