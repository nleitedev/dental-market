import sqlite3
import os

DB_PATH = r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\historico_precos.db"

print("🔧 A reparar banco de dados...")

try:
    # Conectar e executar reparação
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA integrity_check")
    print("✅ Banco OK")
    
    # Otimizar
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("VACUUM")
    print("✅ Banco otimizado")
    conn.close()
    
except sqlite3.Error as e:
    print(f"❌ Erro: {e}")
    print("Tentando recuperar...")
    
    # Fazer backup
    import shutil
    backup = DB_PATH + ".backup"
    shutil.copy(DB_PATH, backup)
    print(f"✅ Backup criado: {backup}")
    
    # Recriar banco
    os.remove(DB_PATH)
    print("✅ Banco antigo removido")
    
    # Criar banco novo
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS precos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data DATE NOT NULL,
            artigo TEXT NOT NULL,
            descricao TEXT,
            concorrente TEXT NOT NULL,
            preco REAL,
            sucesso BOOLEAN DEFAULT 1,
            url TEXT,
            promo BOOLEAN DEFAULT 0,
            data_registo TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_artigo_conc_data ON precos(artigo, concorrente, data DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sucesso ON precos(sucesso)")
    conn.commit()
    conn.close()
    print("✅ Novo banco criado. Execute o scraper para popular com dados.")