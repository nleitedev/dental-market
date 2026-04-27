"""
scraper.py - Obtém preços, stock, promoções e referências a partir dos links
            guardados na base de dados PostgreSQL (Neon) e guarda o histórico
            na tabela 'precos'.

Uso:
    python scraper.py                    # corre todos os concorrentes
    python scraper.py --site PT_Dentaleader # só um concorrente
    python scraper.py --teste 5          # só 5 produtos de cada concorrente (para testar)
"""

import re
import time
import random
import argparse
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
import os
from dotenv import load_dotenv

# ============================================================
# CARREGAR VARIÁVEIS DE AMBIENTE
# ============================================================
load_dotenv()

# ============================================================
# FICHEIRO DE LOG
# ============================================================
LOG_FILE = os.path.join(os.path.dirname(__file__), "scraper_execucoes.log")

def registar_execucao_scraper(site=None, teste=None, sucesso=True, total_ok=0, total_erro=0):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if site:
        modo = f"site={site}"
    else:
        modo = "Todos"
    if teste:
        modo += f" (teste={teste})"
    status = "OK" if sucesso else "ERRO"
    msg = f"[{timestamp}] {modo} - {status} (OK={total_ok}, Erros={total_erro})"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except:
        pass

# ============================================================
# LIGAÇÃO AO POSTGRESQL (Neon)
# ============================================================
def obter_conn_pg():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERRO: Variável DATABASE_URL não definida no ficheiro .env")
        return None
    try:
        conn = psycopg2.connect(database_url)
        return conn
    except Exception as e:
        print(f"ERRO ao ligar ao PostgreSQL: {e}")
        return None

# ============================================================
# GUARDAR PREÇO NA BD
# ============================================================
def guardar_preco(artigo, descricao, concorrente, url, preco, stock, promo, sucesso, erro=None, referencia=None):
    conn = obter_conn_pg()
    if conn is None:
        return
    try:
        # Garantir que promo é um inteiro válido
        try:
            promo_int = int(promo)
        except (TypeError, ValueError):
            promo_int = 0

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO precos (artigo, descricao, concorrente, url, preco, stock, promo, data, sucesso, erro, referencia)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            artigo,
            descricao,
            concorrente,
            url,
            preco,
            stock,
            promo_int,
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            int(sucesso),
            erro,
            referencia
        ))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"ERRO ao guardar preço: {e}")
    finally:
        conn.close()


# ============================================================
# SELENIUM
# ============================================================
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

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
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

def limpar_preco_sem_iva(preco_com_iva):
    try:
        if preco_com_iva and preco_com_iva > 0:
            return round(preco_com_iva / 1.23, 2)
    except:
        pass
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
        erros_titulo = ["404", "405", "not found", "error",
                        "página não encontrada", "page not found"]
        if any(e in titulo for e in erros_titulo):
            return False

        body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        erros_body = ["404", "405", "not found",
                      "página não encontrada", "page not found"]
        sinais_produto = ["preço", "precio", "price", "€", "adicionar", "add to cart",
                          "comprar", "buy", ".base-price", "product"]
        tem_erro = any(e in body_text for e in erros_body)
        tem_produto = any(s in body_text for s in sinais_produto)
        if tem_erro and not tem_produto:
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

# ============================================================
# EXTRACTORES DE PREÇO POR CONCORRENTE
# ============================================================
def extrair_preco_dvddental(driver):
    try:
        elementos_bulk = driver.find_elements(By.CSS_SELECTOR, "#promo .bulk-price, .bulkPrice .bulk-price, .bulk-price")
        for elem in elementos_bulk:
            preco_texto = elem.text.strip()
            if preco_texto:
                match = re.search(r'(\d+)[.,](\d+)', preco_texto)
                if match:
                    preco = limpar_preco(match.group(1), match.group(2))
                    if preco and 0 < preco < 10000:
                        print(f"     [DvdDental] Preço promocional: {preco:.2f}€")
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
                    print(f"     [DvdDental] Preço normal: {preco:.2f}€")
                    return preco, True
    except Exception:
        pass
    
    return None, False

def extrair_preco_dentaleader_pt(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".bulkPrice, .bulk-price, div.bulkPrice"))
        )
        bulk_divs = driver.find_elements(By.CSS_SELECTOR, ".bulkPrice, .bulk-price, div.bulkPrice")
        for bulk in bulk_divs:
            try:
                qtd_elem = bulk.find_element(By.CSS_SELECTOR, ".bulk-qty, .qty")
                qtd_texto = qtd_elem.text.strip()
                print(f"     [Dentaleader] Pack encontrado: {qtd_texto}")
                preco_elem = bulk.find_element(By.CSS_SELECTOR, ".bulk-price, .price")
                preco_texto = preco_elem.text.strip()
                preco_match = re.search(r'(\d+)[.,](\d+)\s*€', preco_texto)
                if preco_match:
                    preco = float(f"{preco_match.group(1)}.{preco_match.group(2)}")
                    print(f"     [Dentaleader] Preço unitário: {preco:.2f}€")
                    return round(preco, 2), True
            except:
                continue
    except Exception:
        pass
    
    try:
        elementos_preco = driver.find_elements(By.CSS_SELECTOR, "span.price, .product-price, .special-price, .regular-price")
        for elem in elementos_preco:
            try:
                pai = elem.find_element(By.XPATH, "..")
                classes_pai = pai.get_attribute("class") or ""
                if "bulk" not in classes_pai.lower():
                    texto = elem.text.strip()
                    match = re.search(r'(\d+)[.,](\d+)\s*€', texto)
                    if match:
                        preco = float(f"{match.group(1)}.{match.group(2)}")
                        if 0 < preco < 10000:
                            print(f"     [Dentaleader] Preço normal: {preco:.2f}€")
                            return round(preco, 2), True
            except:
                continue
    except Exception:
        pass
    
    return None, False

def extrair_preco_minhomedica(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "meta[property='product:pretax_price:amount']"))
        )
        meta_tag = driver.find_element(By.CSS_SELECTOR, "meta[property='product:pretax_price:amount']")
        preco_str = meta_tag.get_attribute("content")
        if preco_str:
            preco = float(preco_str)
            if 0 < preco < 10000:
                return round(preco, 2), True
    except Exception:
        pass
    
    try:
        meta_tag = driver.find_element(By.CSS_SELECTOR, "meta[property='product:price:amount']")
        preco_str = meta_tag.get_attribute("content")
        if preco_str:
            preco_com_iva = float(preco_str)
            if 0 < preco_com_iva < 10000:
                preco_sem_iva = limpar_preco_sem_iva(preco_com_iva)
                return round(preco_sem_iva, 2), True
    except Exception:
        pass
    
    return None, False

def extrair_preco_montellano(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product-final-price"))
        )
        inteiro = driver.find_element(By.CSS_SELECTOR, ".product-final-price .integer-part").text.strip()
        decimal = ""
        try:
            decimal = driver.find_element(By.CSS_SELECTOR,
                        ".product-final-price .decimal-part").text.strip().replace(",", "")
        except Exception:
            texto = driver.find_element(By.CSS_SELECTOR, ".product-final-price").text
            m = re.search(r'(\d+)[.,](\d+)', texto)
            if m:
                inteiro, decimal = m.group(1), m.group(2)
        return limpar_preco(inteiro, decimal), True
    except Exception:
        return None, False

def extrair_preco_dentaltix(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".base-price .base-price-int"))
        )
        inteiro = driver.find_element(By.CSS_SELECTOR, ".base-price .base-price-int").text.strip()
        decimal = "00"
        for el in driver.find_elements(By.CSS_SELECTOR, ".base-price .base-price-dec"):
            txt = el.text.strip().replace(",", "").replace(".", "")
            if txt.isdigit():
                decimal = txt
                break
        if inteiro:
            return limpar_preco(inteiro, decimal), True
    except Exception:
        pass

    try:
        val = driver.execute_script("""
            const i = document.querySelector('.base-price .base-price-int');
            if (!i) return null;
            const ds = Array.from(document.querySelectorAll('.base-price .base-price-dec'))
                            .filter(e => /^\\d+$/.test(e.innerText.trim()));
            return i.innerText.trim() + '.' + (ds.length ? ds[0].innerText.trim() : '00');
        """)
        if val:
            return round(float(val), 2), True
    except Exception:
        pass

    return None, False

def extrair_preco_henryschein_es(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".custom-style-price"))
        )
        preco_texto = driver.find_element(By.CSS_SELECTOR, ".custom-style-price").text.strip()
        match = re.search(r"(\d+)[.,](\d+)", preco_texto)
        if match:
            return limpar_preco(match.group(1), match.group(2)), True
    except Exception:
        pass
    
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.price-wrapper"))
        )
        preco_raw = driver.find_element(By.CSS_SELECTOR, "span.price-wrapper").get_attribute("data-price-amount")
        if preco_raw:
            preco_clean = re.sub(r'[^\d.]', '', preco_raw)
            if preco_clean:
                return round(float(preco_clean), 2), True
    except Exception:
        pass
    
    return None, False

def extrair_preco_henryschein_pt(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span[id^='product-price']"))
        )
        elemento = driver.find_element(By.CSS_SELECTOR, "span[id^='product-price']")
        preco_raw = elemento.get_attribute("data-price-amount")
        if preco_raw:
            preco = float(preco_raw)
            if 0 < preco < 10000:
                print(f"     [HenrySchein PT] data-price-amount: {preco:.2f}€")
                return preco, True
    except Exception:
        pass
    
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span[data-price-type='finalPrice']"))
        )
        elemento = driver.find_element(By.CSS_SELECTOR, "span[data-price-type='finalPrice']")
        preco_raw = elemento.get_attribute("data-price-amount")
        if preco_raw:
            preco = float(preco_raw)
            if 0 < preco < 10000:
                print(f"     [HenrySchein PT] data-price-type finalPrice: {preco:.2f}€")
                return preco, True
    except Exception:
        pass
    
    try:
        elemento = driver.find_element(By.CSS_SELECTOR, "span.price-wrapper span.price")
        preco_texto = elemento.text.strip()
        if preco_texto:
            preco_texto = preco_texto.replace('\xa0', ' ').replace('&nbsp;', ' ')
            match = re.search(r'(\d+)[.,](\d+)', preco_texto)
            if match:
                preco = limpar_preco(match.group(1), match.group(2))
                if preco and 0 < preco < 10000:
                    print(f"     [HenrySchein PT] span.price-wrapper span.price: {preco:.2f}€")
                    return preco, True
    except Exception:
        pass
    
    try:
        elementos = driver.find_elements(By.CSS_SELECTOR, "[data-price-amount]")
        precos = []
        for elem in elementos:
            preco_raw = elem.get_attribute("data-price-amount")
            if preco_raw:
                try:
                    preco = float(preco_raw)
                    if 0 < preco < 10000:
                        precos.append(preco)
                except:
                    pass
        if precos:
            preco = min(precos)
            print(f"     [HenrySchein PT] data-price-amount (menor): {preco:.2f}€")
            return preco, True
    except Exception:
        pass
    
    return None, False

def extrair_preco_dentalexpress_es(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "meta[itemprop='price']"))
        )
        preco_str = driver.find_element(By.CSS_SELECTOR, "meta[itemprop='price']").get_attribute("content")
        if preco_str:
            preco = float(preco_str)
            if 0 < preco < 10000:
                return round(preco, 2), True
    except Exception:
        pass
    
    try:
        WebDriverWait(driver, 4).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product-card__price--final"))
        )
        preco_texto = driver.find_element(By.CSS_SELECTOR, ".product-card__price--final").text.strip()
        match = re.search(r"(\d+)[.,](\d+)", preco_texto)
        if match:
            return limpar_preco(match.group(1), match.group(2)), True
    except Exception:
        pass
    
    return None, False

def extrair_preco_dentalexpress_pt(driver):
    try:
        WebDriverWait(driver, 6).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "meta[itemprop='price']"))
        )
        preco_str = driver.find_element(By.CSS_SELECTOR, "meta[itemprop='price']").get_attribute("content")
        if preco_str:
            preco = float(preco_str)
            if 0 < preco < 10000:
                print(f"     [DentalExpress PT] Meta tag: {preco:.2f}€")
                return round(preco, 2), True
    except Exception:
        pass
    
    try:
        page_source = driver.page_source
        match = re.search(r'(\d+),<sup>(\d+)</sup>', page_source)
        if match:
            preco = limpar_preco(match.group(1), match.group(2))
            if preco and 0 < preco < 10000:
                print(f"     [DentalExpress PT] HTML com sup: {preco:.2f}€")
                return preco, True
    except Exception:
        pass
    
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "span.price"))
        )
        elemento = driver.find_element(By.CSS_SELECTOR, "span.price")
        
        try:
            html_interno = elemento.get_attribute("innerHTML")
            if html_interno:
                html_limpo = re.sub(r'<sup>', '', html_interno)
                html_limpo = re.sub(r'</sup>', '', html_limpo)
                html_limpo = html_limpo.replace('&nbsp;', ' ')
                match = re.search(r'(\d+)[.,](\d+)', html_limpo)
                if match:
                    preco = limpar_preco(match.group(1), match.group(2))
                    if preco and 0 < preco < 10000:
                        print(f"     [DentalExpress PT] Span com sup: {preco:.2f}€")
                        return preco, True
        except:
            pass
        
        preco_texto = elemento.text.strip()
        if preco_texto:
            preco_texto = preco_texto.replace(' ', '').replace('&nbsp;', '')
            match = re.search(r'(\d+)[.,](\d+)', preco_texto)
            if match:
                preco = limpar_preco(match.group(1), match.group(2))
                if preco and 0 < preco < 10000:
                    print(f"     [DentalExpress PT] Span normal: {preco:.2f}€")
                    return preco, True
    except Exception:
        pass
    
    return None, False

def extrair_preco_generico(driver):
    for sel in [".price--withoutTax", 
                "[data-product-price-without-tax]",
                ".withoutTax",
                ".product-price span:not(.hs-strike)", 
                ".custom-style-price",
                ".price-wrapper", 
                ".product-final-price", 
                ".special-price", 
                ".price",
                "meta[itemprop='price']",
                "span[id^='product-price']",
                "span[data-price-type='finalPrice']"]:
        try:
            if sel == "meta[itemprop='price']":
                elemento = driver.find_element(By.CSS_SELECTOR, sel)
                preco_str = elemento.get_attribute("content")
                if preco_str:
                    p = float(preco_str)
                    if 0 < p < 50000:
                        return p, True
            elif sel in (".price--withoutTax", "[data-product-price-without-tax]", ".withoutTax"):
                try:
                    if sel == ".price--withoutTax":
                        elemento = driver.find_element(By.CSS_SELECTOR, sel)
                        texto = elemento.text.strip()
                        match = re.search(r'(\d+)[.,](\d+)', texto)
                        if match:
                            p = limpar_preco(match.group(1), match.group(2))
                            if p and 0 < p < 50000:
                                return p, True
                    elif sel == "[data-product-price-without-tax]":
                        elemento = driver.find_element(By.CSS_SELECTOR, sel)
                        preco_str = elemento.get_attribute("data-product-price-without-tax")
                        if preco_str:
                            p = float(preco_str.replace(",", "."))
                            if 0 < p < 50000:
                                return round(p, 2), True
                    elif sel == ".withoutTax":
                        elemento = driver.find_element(By.CSS_SELECTOR, sel)
                        texto = elemento.text.strip()
                        match = re.search(r'(\d+)[.,](\d+)', texto)
                        if match:
                            p = limpar_preco(match.group(1), match.group(2))
                            if p and 0 < p < 50000:
                                return p, True
                except:
                    pass
            elif "data-price-amount" in sel or "id^=" in sel:
                elementos = driver.find_elements(By.CSS_SELECTOR, sel)
                for elem in elementos:
                    preco_raw = elem.get_attribute("data-price-amount")
                    if preco_raw:
                        p = float(re.sub(r'[^\d.]', '', preco_raw))
                        if 0 < p < 50000:
                            return p, True
            else:
                elementos = driver.find_elements(By.CSS_SELECTOR, sel)
                for elem in elementos:
                    texto = elem.text.strip()
                    classes = elem.get_attribute("class") or ""
                    if "strike" in classes.lower() or "old" in classes.lower():
                        continue
                    match = re.search(r'(\d+)[.,](\d+)', texto)
                    if match:
                        p = limpar_preco(match.group(1), match.group(2))
                        if p and 0 < p < 50000:
                            return p, True
        except Exception:
            continue
    
    try:
        texto = driver.find_element(By.TAG_NAME, "body").text
        matches = re.findall(r'(\d+)[.,](\d+)\s*€', texto)
        precos = []
        for m in matches:
            p = limpar_preco(m[0], m[1])
            if p and 0 < p < 50000:
                precos.append(p)
        if precos:
            return min(precos), True
    except Exception:
        pass
    
    return None, False

# Mapa de extractores de preço
EXTRATORES = {
    "es_dvddental":         extrair_preco_dvddental,
    "dvddental":            extrair_preco_dvddental,
    "es_dvd-dental":        extrair_preco_dvddental,
    "dvd-dental":           extrair_preco_dvddental,
    "es_dvd_dental":        extrair_preco_dvddental,
    
    "dentaleader":          extrair_preco_dentaleader_pt,
    "pt_dentaleader":       extrair_preco_dentaleader_pt,
    "pt_dentaleader_pt":    extrair_preco_dentaleader_pt,
    
    "minhomedica":          extrair_preco_minhomedica,
    "pt_minhomédica":       extrair_preco_minhomedica,
    "pt_minhomedica":       extrair_preco_minhomedica,
    
    "montellano":           extrair_preco_montellano,
    "pt_montellano":        extrair_preco_montellano,
    
    "es_dentaltix":         extrair_preco_dentaltix,
    "dentaltix":            extrair_preco_dentaltix,
    
    "es_henryschein":       extrair_preco_henryschein_es,
    "henryschein_es":       extrair_preco_henryschein_es,
    "pt_henryschein":       extrair_preco_henryschein_pt,
    "henryschein_pt":       extrair_preco_henryschein_pt,
    
    "es_dentalexpress":     extrair_preco_dentalexpress_es,
    "dentalexpress_es":     extrair_preco_dentalexpress_es,
    "pt_dentalexpress":     extrair_preco_dentalexpress_pt,
    "dentalexpress_pt":     extrair_preco_dentalexpress_pt,
}

# ============================================================
# EXTRACTORES DE REFERÊNCIA
# ============================================================
def extrair_referencia_generico(driver):
    """Tenta extrair SKU ou referência de locais comuns."""
    for meta_sel in ["meta[itemprop='sku']", "meta[property='product:retailer_item_id']"]:
        try:
            meta = driver.find_element(By.CSS_SELECTOR, meta_sel)
            ref = meta.get_attribute("content")
            if ref and ref.strip():
                return ref.strip()
        except:
            pass

    for classe in [".sku", ".reference", ".product-reference", ".product-sku", ".ref", "[itemprop='sku']"]:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, classe)
            texto = elem.text.strip()
            if texto:
                texto = re.sub(r'(?i)^(ref(erence|erência)?|sku|code|código|art(igo)?)\s*[:.]?\s*', '', texto)
                return texto[:50]
        except:
            continue
    return None


def extrair_referencia_nenhuma(driver):
    return None


def extrair_referencia_dvddental(driver):
    try:
        tbody = driver.find_element(By.CSS_SELECTOR, "#variants-section")
        linhas = tbody.find_elements(By.CSS_SELECTOR, "tr.variant-row")
        if linhas:
            print("     [DvdDental] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".productView-info-value--sku"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".productView-info-value--sku")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    try:
        elem = driver.find_element(By.CSS_SELECTOR, ".sku, .product-sku")
        texto = elem.text.strip()
        if texto:
            return re.sub(r'(?i)^ref[:.\s]*', '', texto).strip()
    except:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_dentaleader_pt(driver):
    try:
        tbody = driver.find_element(By.CSS_SELECTOR, "#variants-section")
        linhas = tbody.find_elements(By.CSS_SELECTOR, "tr.variant-row")
        if linhas:
            print("     [Dentaleader PT] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".productView-info-value--sku"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".productView-info-value--sku")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_minhomedica(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product-reference span[itemprop='sku']"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".product-reference span[itemprop='sku']")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass
    return extrair_referencia_generico(driver)


def extrair_referencia_montellano(driver):
    try:
        sku_elements = driver.find_elements(By.CSS_SELECTOR, ".product-view__products-table-row-reference--sku")
        if len(sku_elements) > 1:
            print("     [Montellano] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".ref-proclinic"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".ref-proclinic")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    try:
        elem = driver.find_element(By.CSS_SELECTOR, ".product-sku, .sku")
        return elem.text.strip()
    except:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_dentaltix(driver):
    try:
        produtos = driver.find_elements(By.CSS_SELECTOR, ".product-variations-list .listed-product")
        if len(produtos) > 1:
            print("     [Dentaltix] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        elem = driver.find_element(By.XPATH, "//span[contains(text(),'Referencia:')]/following-sibling::span/strong")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    try:
        elem = driver.find_element(By.CSS_SELECTOR, ".title-price div span strong")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_henryschein_es(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product-title"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".product-title small strong")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass
    return extrair_referencia_generico(driver)


def extrair_referencia_henryschein_pt(driver):
    try:
        tabela = driver.find_element(By.CSS_SELECTOR, "#configurable-product-table")
        if tabela:
            print("     [HenrySchein PT] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product.attribute.sku .value"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".product.attribute.sku .value")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    try:
        elem = driver.find_element(By.CSS_SELECTOR, "[itemprop='sku']")
        texto = elem.text.strip() or elem.get_attribute("content")
        if texto:
            return texto.strip()
    except:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_dentalexpress(driver):
    try:
        linhas = driver.find_elements(By.CSS_SELECTOR, "tr.configurable-items")
        if linhas:
            print("     [DentalExpress] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product.attribute.sku.de .value"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".product.attribute.sku.de .value")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    try:
        elem = driver.find_element(By.CSS_SELECTOR, "[itemprop='sku']")
        texto = elem.text.strip() or elem.get_attribute("content")
        if texto:
            return texto.strip()
    except:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_proclinic(driver):
    try:
        sku_elements = driver.find_elements(By.CSS_SELECTOR, ".product-view__products-table-row-reference--sku")
        if len(sku_elements) > 1:
            print("     [Proclinic] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".ref-proclinic"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".ref-proclinic")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_royaldent(driver):
    try:
        select_elem = driver.find_element(By.CSS_SELECTOR, "select.text_field.formatat.first_null")
        opcoes = select_elem.find_elements(By.TAG_NAME, "option")
        opcoes_validas = [opt for opt in opcoes if opt.get_attribute("value") != ""]
        if len(opcoes_validas) > 1:
            print("     [RoyalDent] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product_description p strong"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".product_description p strong")
        texto = elem.text.strip()
        texto = re.sub(r'(?i)^referencia\s*[:]?\s*', '', texto)
        if texto:
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_dontalia(driver):
    try:
        linhas = driver.find_elements(By.CSS_SELECTOR, "#products-table .product-view__product-row")
        if len(linhas) > 1:
            print("     [Dontalia] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product-data__sku"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".product-data__sku")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_bnh(driver):
    try:
        select_elem = driver.find_element(By.CSS_SELECTOR, ".variations_form select")
        opcoes = select_elem.find_elements(By.TAG_NAME, "option")
        opcoes_validas = [opt for opt in opcoes if opt.get_attribute("value") != ""]
        if len(opcoes_validas) > 1:
            print("     [BNH] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".sku_wrapper .sku"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".sku_wrapper .sku")
        texto = elem.text.strip()
        if texto and texto != "n.d.":
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_exomed(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#product_reference span.editable"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, "#product_reference span.editable")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass
    return extrair_referencia_generico(driver)


def extrair_referencia_tropicofuturo(driver):
    try:
        select_elem = driver.find_element(By.CSS_SELECTOR, ".variations_form select")
        opcoes = select_elem.find_elements(By.TAG_NAME, "option")
        opcoes_validas = [opt for opt in opcoes if opt.get_attribute("value") != ""]
        if len(opcoes_validas) > 1:
            print("     [TropicoFuturo] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".sku_wrapper .sku"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".sku_wrapper .sku")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_tacasdental(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".product.attribute.sku .value"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".product.attribute.sku .value")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass
    return extrair_referencia_generico(driver)


def extrair_referencia_nordental(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".sku_wrapper .sku"))
        )
        elem = driver.find_element(By.CSS_SELECTOR, ".sku_wrapper .sku")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass
    return extrair_referencia_generico(driver)


def extrair_referencia_uppermat(driver):
    try:
        linhas = driver.find_elements(By.CSS_SELECTOR, ".variations-table tbody tr")
        if len(linhas) > 1:
            print("     [Uppermat] Múltiplas variantes detectadas – referência omitida.")
            return None
        elif len(linhas) == 1:
            ref_elem = linhas[0].find_element(By.CSS_SELECTOR, "td:first-child")
            texto = ref_elem.text.strip()
            if texto:
                return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


def extrair_referencia_dentaliberica(driver):
    try:
        linhas = driver.find_elements(By.CSS_SELECTOR, ".product-view__products-table-row")
        if len(linhas) > 1:
            print("     [DentalIberica] Múltiplas variantes detectadas – referência omitida.")
            return None
    except Exception:
        pass

    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//span[contains(text(),'Referencia:')]/following-sibling::span[@class='product-info__info-value']"))
        )
        elem = driver.find_element(By.XPATH, "//span[contains(text(),'Referencia:')]/following-sibling::span[@class='product-info__info-value']")
        texto = elem.text.strip()
        if texto:
            return texto
    except Exception:
        pass

    return extrair_referencia_generico(driver)


# Mapeamento de extractores de referência
REFERENCIA_EXTRATORES = {
    "es_dvddental": extrair_referencia_dvddental,
    "dvddental": extrair_referencia_dvddental,
    "es_dvd-dental": extrair_referencia_dvddental,
    "dvd-dental": extrair_referencia_dvddental,
    "es_dvd_dental": extrair_referencia_dvddental,

    "pt_dentaleader": extrair_referencia_dentaleader_pt,
    "dentaleader": extrair_referencia_dentaleader_pt,

    "pt_minhomedica": extrair_referencia_minhomedica,
    "minhomedica": extrair_referencia_minhomedica,

    "pt_montellano": extrair_referencia_montellano,
    "montellano": extrair_referencia_montellano,

    "es_dentaltix": extrair_referencia_dentaltix,
    "dentaltix": extrair_referencia_dentaltix,

    "es_henryschein": extrair_referencia_henryschein_es,
    "henryschein_es": extrair_referencia_henryschein_es,

    "pt_henryschein": extrair_referencia_henryschein_pt,
    "henryschein_pt": extrair_referencia_henryschein_pt,

    "es_dentalexpress": extrair_referencia_dentalexpress,
    "dentalexpress_es": extrair_referencia_dentalexpress,

    "pt_dentalexpress": extrair_referencia_dentalexpress,
    "dentalexpress_pt": extrair_referencia_dentalexpress,

    "es_proclinic": extrair_referencia_proclinic,
    "proclinic": extrair_referencia_proclinic,
    "pt_proclinic": extrair_referencia_proclinic,

    "es_royaldent": extrair_referencia_royaldent,
    "royaldent": extrair_referencia_royaldent,
    "royal-dent": extrair_referencia_royaldent,
    "es_royal-dent": extrair_referencia_royaldent,

    "pt_dontalia": extrair_referencia_dontalia,
    "dontalia": extrair_referencia_dontalia,

    "pt_bnh": extrair_referencia_bnh,
    "bnh": extrair_referencia_bnh,

    "pt_exomed": extrair_referencia_exomed,
    "exomed": extrair_referencia_exomed,

    "pt_tropicofuturo": extrair_referencia_tropicofuturo,
    "tropicofuturo": extrair_referencia_tropicofuturo,

    "pt_tacasdental": extrair_referencia_tacasdental,
    "tacasdental": extrair_referencia_tacasdental,

    "pt_nordental": extrair_referencia_nordental,
    "nordental": extrair_referencia_nordental,

    "es_uppermat": extrair_referencia_uppermat,
    "uppermat": extrair_referencia_uppermat,

    "es_dentaliberica": extrair_referencia_dentaliberica,
    "dentaliberica": extrair_referencia_dentaliberica,

    "pt_augustocabral": extrair_referencia_nenhuma,
    "augustocabral": extrair_referencia_nenhuma,
    "pt_dotamed": extrair_referencia_nenhuma,
    "dotamed": extrair_referencia_nenhuma,
    "pt_nooldental": extrair_referencia_nenhuma,
    "nooldental": extrair_referencia_nenhuma,
}

# ============================================================
# FUNÇÃO PRINCIPAL DE SCRAPE
# ============================================================

def scrape_url(driver, url, concorrente):
    if not url_valida(url):
        return None, None, None, False, "URL inválido"
    
    try:
        # Timeout para evitar que o scraper fique preso
        driver.set_page_load_timeout(20)
        driver.get(url)
    except TimeoutException:
        return None, None, None, False, "Timeout ao carregar página"
    except Exception as e:
        return None, None, None, False, f"Erro ao carregar: {str(e)[:80]}"

    time.sleep(2)  # tempo reduzido

    if not pagina_valida(driver):
        return None, None, None, False, "Página não encontrada"

    # Tentar fechar pop‑ups comuns (cookies)
    try:
        # Procurar botões de aceitar cookies com selectores comuns
        cookie_btns = driver.find_elements(By.CSS_SELECTOR, 
            "[id*='accept'], [class*='accept'], [id*='cookie'], [class*='cookie'], "
            "[aria-label*='cookie'], [aria-label*='Cookie'], "
            "button:contains('Accept'), button:contains('Aceitar'), button:contains('OK')")
        for btn in cookie_btns[:3]:  # tenta no máximo 3 botões
            try:
                btn.click()
                time.sleep(1)
            except:
                pass
    except:
        pass
    
    # Preço
    extrator = EXTRATORES.get(concorrente.lower(), extrair_preco_generico)
    preco, ok = extrator(driver)
    if not ok or preco is None:
        preco, ok = extrair_preco_generico(driver)
    
    # Referência
    ref_extrator = REFERENCIA_EXTRATORES.get(concorrente.lower(), extrair_referencia_generico)
    referencia = ref_extrator(driver)
    
    stock = verificar_stock(driver)
    promo = verificar_promo(driver)
    
    if preco:
        return preco, stock, promo, referencia, None
    return None, stock, promo, referencia, "Preço não extraído"

# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site",  type=str, default=None, help="Nome do concorrente (ex: PT_Dentaleader)")
    parser.add_argument("--teste", type=int, default=None, help="Limitar a N produtos (para teste)")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  DENTAL MONITOR - Scraper de Precos")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"{'='*55}\n")

    # Obter concorrentes ativos da BD
    conn_list = obter_conn_pg()
    if conn_list is None:
        return

    try:
        cur = conn_list.cursor()
        if args.site:
            cur.execute("SELECT nome FROM concorrentes WHERE ativo = 1 AND nome = %s", (args.site,))
        else:
            cur.execute("SELECT nome FROM concorrentes WHERE ativo = 1 ORDER BY nome")
        concorrentes = [row[0] for row in cur.fetchall()]
        cur.close()
    except Exception as e:
        print(f"ERRO ao obter concorrentes: {e}")
        conn_list.close()
        return
    conn_list.close()

    if not concorrentes:
        print("Nenhum concorrente ativo encontrado.")
        return

    print(f"Processando: {', '.join(concorrentes)}\n")

    driver = iniciar_driver()
    total_ok = 0
    total_erro = 0

    for conc in concorrentes:
        print(f"\n>> {conc}")
        conn_links = obter_conn_pg()
        if conn_links is None:
            continue
        try:
            cur = conn_links.cursor()
            cur.execute("""
                SELECT l.artigo, a.descricao, l.url
                FROM links l
                JOIN artigos a ON l.artigo = a.artigo
                WHERE l.concorrente = %s AND l.url IS NOT NULL AND l.url != ''
                ORDER BY l.artigo
            """, (conc,))
            links = cur.fetchall()
            cur.close()
        except Exception as e:
            print(f"ERRO ao obter links para {conc}: {e}")
            conn_links.close()
            continue
        conn_links.close()

        if args.teste:
            links = links[:args.teste]
            print(f"  [TESTE: {args.teste} produtos]")

        sheet_ok = 0
        sheet_erro = 0

        for artigo, descricao, url in links:
            if not artigo or artigo == "nan" or artigo == "":
                continue
            if not url_valida(url):
                continue

            print(f"  A processar {artigo}...")
            preco, stock, promo, referencia, erro = scrape_url(driver, url, conc)

            guardar_preco(artigo, descricao, conc, url,
                          preco, stock, promo,
                          sucesso=(preco is not None), erro=erro, referencia=referencia)

            if preco:
                ref_str = f" ({referencia})" if referencia else ""
                print(f"  OK {artigo:15}  {preco:8.2f}€  {stock}  {'PROMO' if promo else ''}{ref_str}")
                sheet_ok += 1
                total_ok += 1
            else:
                print(f"  ERRO {artigo:15}  {erro}")
                sheet_erro += 1
                total_erro += 1

            time.sleep(random.uniform(1.5, 3.0))

        if sheet_ok > 0 or sheet_erro > 0:
            print(f"  RES {conc}: OK {sheet_ok}  ERRO {sheet_erro}")
        time.sleep(2)

    driver.quit()

    registar_execucao_scraper(
        site=args.site,
        teste=args.teste,
        sucesso=(total_erro == 0),
        total_ok=total_ok,
        total_erro=total_erro
    )

    print(f"\n{'='*55}")
    print(f"  SUCESSO: {total_ok}")
    print(f"  ERROS:   {total_erro}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()