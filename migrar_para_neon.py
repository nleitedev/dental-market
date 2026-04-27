import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
import openpyxl

# ========== CONFIGURAÇÃO ==========
EXCEL_PATH = r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\links_concorrentes.xlsx"
DB_PATH_SQLITE = r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\historico_precos.db"

# COLOQUE AQUI A STRING DE CONEXÃO REAL DO NEON
NEON_DB_URL = "postgresql://neondb_owner:npg_nbY4vhm2qdJl@ep-green-fire-abswljp7.eu-west-2.aws.neon.tech/neondb?sslmode=require"

engine = create_engine(NEON_DB_URL)

# ---------- 1. Migrar catálogo Douromed ----------
print("📦 Migrando catálogo Douromed...")
df_dm = pd.read_excel(EXCEL_PATH, sheet_name="Douromed", dtype=str)
df_dm.columns = ["artigo", "descricao", "pvp1", "stk_actual", "stk_reposicao",
                 "marca", "familia", "ref_fornecedor", "atualizado_em"]
df_dm["pvp1"] = pd.to_numeric(df_dm["pvp1"], errors="coerce")
df_dm["stk_actual"] = pd.to_numeric(df_dm["stk_actual"], errors="coerce")
df_dm["stk_reposicao"] = pd.to_numeric(df_dm["stk_reposicao"], errors="coerce")

# Limpar tabela antes de inserir
with engine.connect() as conn:
    conn.execute(text("DELETE FROM artigos"))
    conn.commit()

df_dm.to_sql("artigos", engine, if_exists="append", index=False)
print("   ✅ Artigos migrados.")

# ---------- 2. Migrar concorrentes e links ----------
print("🔗 Migrando concorrentes e links...")
xls = pd.ExcelFile(EXCEL_PATH)
with engine.connect() as conn:
    # Limpar tabelas para evitar duplicados em execuções repetidas
    conn.execute(text("DELETE FROM links"))
    conn.execute(text("DELETE FROM concorrentes"))
    conn.commit()

    for sheet in xls.sheet_names:
        if sheet.lower() == "douromed":
            continue
        # Inserir concorrente
        conn.execute(
            text("INSERT INTO concorrentes (nome, homepage, ativo) VALUES (:nome, '', 1) ON CONFLICT (nome) DO NOTHING"),
            {"nome": sheet}
        )
        
        df = pd.read_excel(EXCEL_PATH, sheet_name=sheet, dtype=str).fillna("")
        df.columns = df.columns.str.strip()
        col_art = df.columns[0]
        col_url = df.columns[2] if len(df.columns) > 2 else None
        col_ref_manual = df.columns[3] if len(df.columns) > 3 else None
        
        if col_url:
            for _, row in df.iterrows():
                artigo = str(row[col_art]).strip()
                url = str(row[col_url]).strip() if row[col_url] and str(row[col_url]).startswith("http") else ""
                ref_manual = str(row[col_ref_manual]).strip() if col_ref_manual and row[col_ref_manual] else ""
                if url or ref_manual:
                    conn.execute(
                        text("""
                            INSERT INTO links (artigo, concorrente, url, referencia_manual)
                            VALUES (:artigo, :concorrente, :url, :ref_manual)
                            ON CONFLICT (artigo, concorrente) DO UPDATE
                            SET url = EXCLUDED.url, referencia_manual = EXCLUDED.referencia_manual
                        """),
                        {"artigo": artigo, "concorrente": sheet, "url": url, "ref_manual": ref_manual}
                    )
    conn.commit()
print("   ✅ Concorrentes e links migrados.")

# ---------- 3. Migrar preços (SQLite) ----------
print("📈 Migrando histórico de preços...")
sqlite_conn = sqlite3.connect(DB_PATH_SQLITE)
df_precos = pd.read_sql("SELECT * FROM precos", sqlite_conn)
df_precos.to_sql("precos", engine, if_exists="append", index=False, method="multi")
print("   ✅ Preços migrados.")

print("\n🎉 Migração concluída!")