import sqlite3
import streamlit as st

DB_PATH = r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\historico_precos.db"

# Limpar cache do Streamlit
st.cache_data.clear()

# Conectar e remover definitivamente
conn = sqlite3.connect(DB_PATH)

# Remover todos os registos de concorrentes antigos que não interessam
concorrentes_remover = [
    'PT_DentalFuturo', 'PT_NoolDental', 'PT_Nordental', 
    'PT_TacasDental', 'PT_TropicoFuturo'
]

for conc in concorrentes_remover:
    conn.execute(f"DELETE FROM precos WHERE concorrente = '{conc}'")
    print(f"Removido: {conc}")

conn.commit()

# Verificar concorrentes restantes
cursor = conn.execute("SELECT DISTINCT concorrente FROM precos ORDER BY concorrente")
print("\nConcorrentes restantes na BD:")
for r in cursor.fetchall():
    print(f"  - {r[0]}")

conn.close()
print("\n✅ Limpeza concluída! Reinicie o dashboard.")