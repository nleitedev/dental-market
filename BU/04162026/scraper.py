"""
scraper.py - Lê links do Excel, extrai preços e guarda histórico em SQLite.
"""

import re
import time
import random
import sqlite3
import argparse
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import os

# ─────────────────────────────────────────────
#  CAMINHOS
# ─────────────────────────────────────────────

EXCEL_LINKS = r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\links_concorrentes.xlsx"
DB_PATH     = r"D:\ProjPREÇOSCONCORRENCIA\Emdesenvolvimento\historico_precos.db"

# ─────────────────────────────────────────────
#  BASE DE DADOS
# ─────────────────────────────────────────────

def iniciar_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS precos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            artigo      TEXT NOT NULL,
            descricao   TEXT,
            concorrente TEXT NOT NULL,
            url         TEXT,
            preco       REAL,
            stock       TEXT,
            promo       INTEGER DEFAULT 0,
            data        TEXT NOT NULL,
            sucesso     INTEGER DEFAULT 1,
            erro        TEXT
        )
    """)
    conn.commit()
    return conn

def guardar_preco(conn, artigo, descricao, concorrente, url,
                  preco, stock, promo, sucesso, erro=None):
    conn.execute("""
        INSERT INTO precos
            (artigo, descricao, concorrente, url, preco, stock, promo, data, sucesso, erro)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (artigo, descricao, concorrente, url, preco, stock,
          int(promo), datetime.now().strftime("%Y-%m-%d %H:%M"),
          int(sucesso), erro))
    conn.commit()

# ─────────────────────────────────────────────
#  SELENIUM
# ─────────────────────────────────────────────

def iniciar_driver():
    print("  A iniciar Chrome...")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--log-level=3')
    options.add_argument('--disable-images')
    options.add_argument('--disable-extensions')
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(30)
    print("  Chrome iniciado!")
    return driver

# ─────────────────────────────────────────────
#  FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────

def limpar_preco(inteiro, decimal):
    try:
        i = re.sub(r'[^\d]', '', str(inteiro))
        d = re.sub(r'[^\d]', '', str(decimal))
        if len(d) == 0:   d = "00"
        elif len(d) == 1: d += "0"
        elif len(d) > 2:  d = d[:2]
        return round(float(f"{i}.{d}"), 2)
    except Exception:
        return None

def verificar_stock(driver):
    try:
        texto = driver.find_element(By.TAG_NAME, "body").text.lower()
        if any(x in texto for x in ["sem stock", "esgotado", "indisponível", "out of stock"]):
            return "indisponivel"
        if any(x in texto for x in ["em stock", "disponível", "in stock", "disponivel"]):
            return "disponivel"
    except Exception:
        pass
    return "desconhecido"

def verificar_promo(driver):
    try:
        texto = driver.find_element(By.TAG_NAME, "body").text.lower()
        return any(x in texto for x in ["promoção", "promocao", "desconto", "oferta", "-%", "sale"])
    except Exception:
        return False

def pagina_valida(driver):
    try:
        titulo = driver.title.lower()
        if any(e in titulo for e in ["404", "405", "not found", "error"]):
            return False
        return True
    except Exception:
        return False

def url_valida(url):
    if not isinstance(url, str):
        return False
    url = url.strip()
    if not url or url == "nan" or url == "None" or url == "":
        return False
    if not url.startswith(("http://", "https://")):
        return False
    return True

# ─────────────────────────────────────────────
#  EXTRACTORES DE PREÇO
# ─────────────────────────────────────────────

def extrair_preco_dvddental(driver):
    """Extracção específica para ES_DvdDental."""
    try:
        elementos_bulk = driver.find_elements(By.CSS_SELECTOR, "#promo .bulk-price, .bulkPrice .bulk-price, .bulk-price")
        for elem in elementos_bulk:
            preco_texto = elem.text.strip()
            if preco_texto:
                match = re.search(r'(\d+)[.,](\d+)', preco_texto)
                if match:
                    preco = limpar_preco(match.group(1), match.group(2))
                    if preco and 0 < preco < 10000:
                        return preco, True
    except Exception:
        pass
    
    try:
        elementos_final = driver.find_elements(By.CSS_SELECTOR, ".final-price.pink, .final-price")
        for elem in elementos_final:
            preco_texto = elem.text.strip()
            match = re.search(r'(\d+)[.,](\d+)', preco_texto)
            if match:
                preco = limpar_preco(match.group(1), match.group(2))
                if preco and 0 < preco < 10000:
                    return preco, True
    except Exception:
        pass
    
    return None, False

def extrair_preco_generico(driver):
    for sel in [".price", ".product-price", ".special-price", "span.price"]:
        try:
            elementos = driver.find_elements(By.CSS_SELECTOR, sel)
            for elem in elementos:
                texto = elem.text.strip()
                match = re.search(r'(\d+)[.,](\d+)', texto)
                if match:
                    p = limpar_preco(match.group(1), match.group(2))
                    if p and 0 < p < 10000:
                        return p, True
        except Exception:
            continue
    
    try:
        texto = driver.find_element(By.TAG_NAME, "body").text
        matches = re.findall(r'(\d+)[.,](\d+)\s*€', texto)
        for m in matches:
            p = limpar_preco(m[0], m[1])
            if p and 0 < p < 10000:
                return p, True
    except Exception:
        pass
    
    return None, False

# Mapa de extractores
EXTRATORES = {
    "es_dvddental": extrair_preco_dvddental,
    "dvddental": extrair_preco_dvddental,
}

# ─────────────────────────────────────────────
#  FUNÇÃO PRINCIPAL DE SCRAPE
# ─────────────────────────────────────────────

def scrape_url(driver, url, concorrente):
    if not url_valida(url):
        return None, None, False, "URL inválido"
    
    try:
        driver.get(url)
        time.sleep(2)
        
        if not pagina_valida(driver):
            return None, None, False, "Página não encontrada"
        
        extrator = EXTRATORES.get(concorrente.lower(), extrair_preco_generico)
        preco, ok = extrator(driver)
        
        if not ok or preco is None:
            preco, ok = extrair_preco_generico(driver)
        
        stock = verificar_stock(driver)
        promo = verificar_promo(driver)
        
        if preco:
            return preco, stock, promo, None
        return None, stock, promo, "Preço não extraído"
    except TimeoutException:
        return None, None, False, "Timeout"
    except Exception as e:
        return None, None, False, str(e)[:80]

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site",  type=str, default=None, help="Nome do separador")
    parser.add_argument("--teste", type=int, default=None, help="Limitar a N produtos")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  DENTAL MONITOR - Scraper de Precos")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*55}\n")

    try:
        xl = pd.ExcelFile(EXCEL_LINKS)
    except Exception as e:
        print(f"ERRO ao abrir Excel: {e}")
        return

    sheets = [s for s in xl.sheet_names if s.lower() != "douromed"]
    
    if args.site:
        if args.site not in sheets:
            print(f"ERRO: Separador '{args.site}' nao encontrado. Disponiveis: {sheets}")
            return
        sheets = [args.site]

    if not sheets:
        print("Nenhum separador para processar")
        return

    print(f"Processando: {', '.join(sheets)}\n")

    conn = iniciar_db()
    driver = iniciar_driver()
    
    total_ok = 0
    total_erro = 0

    for sheet in sheets:
        print(f"\n>> {sheet}")
        
        df = pd.read_excel(EXCEL_LINKS, sheet_name=sheet, dtype=str).fillna("")
        df.columns = df.columns.str.strip()
        col_map = {c.lower(): c for c in df.columns}
        col_art = col_map.get("artigo", df.columns[0])
        col_desc = col_map.get("descricao", df.columns[1] if len(df.columns) > 1 else None)
        col_url = col_map.get("url", df.columns[2] if len(df.columns) > 2 else None)

        if args.teste:
            df = df.head(args.teste)
            print(f"  [TESTE: {args.teste} produtos]")
        
        sheet_ok = 0
        sheet_erro = 0

        for idx, row in df.iterrows():
            artigo = str(row[col_art]).strip()
            descricao = str(row[col_desc]).strip() if col_desc else ""
            url = str(row[col_url]).strip() if col_url else ""

            if not artigo or artigo == "nan" or artigo == "":
                continue

            if not url_valida(url):
                continue

            print(f"  A processar {artigo}...")
            preco, stock, promo, erro = scrape_url(driver, url, sheet)

            guardar_preco(conn, artigo, descricao, sheet, url,
                          preco, stock, promo,
                          sucesso=(preco is not None), erro=erro)

            if preco:
                print(f"  OK {artigo} -> {preco:.2f}€")
                sheet_ok += 1
                total_ok += 1
            else:
                print(f"  ERRO {artigo} -> {erro}")
                sheet_erro += 1
                total_erro += 1

            time.sleep(random.uniform(1, 2))

        print(f"  RES {sheet}: OK {sheet_ok} | ERRO {sheet_erro}")

    driver.quit()
    conn.close()

    print(f"\n{'='*55}")
    print(f"  SUCESSO: {total_ok}")
    print(f"  ERROS:   {total_erro}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
    