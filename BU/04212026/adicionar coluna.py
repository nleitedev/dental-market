import sqlite3
conn = sqlite3.connect(r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\historico_precos.db")
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(precos)")
colunas = [col[1] for col in cursor.fetchall()]
if 'referencia' not in colunas:
    cursor.execute("ALTER TABLE precos ADD COLUMN referencia TEXT")
    conn.commit()
    print("Coluna 'referencia' adicionada.")
else:
    print("Coluna 'referencia' já existe.")
conn.close()