# check_db.py
import sqlite3

DB_PATH = r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\historico_precos.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.execute("SELECT COUNT(*) FROM precos WHERE sucesso=1")
count = cursor.fetchone()[0]
print(f"Registos na tabela precos: {count}")

cursor = conn.execute("SELECT DISTINCT artigo FROM precos WHERE sucesso=1 LIMIT 5")
artigos = cursor.fetchall()
print(f"Exemplo de artigos: {[a[0] for a in artigos]}")

conn.close()