import os
import re
import csv
import requests
import datetime
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text
from io import BytesIO
import pandas as pd

BASE_DIR = "./output"
os.makedirs(BASE_DIR, exist_ok=True)

# Define a lista de palavras-chave que indicam criação de eventos
KEYWORDS = ["prêmio", "waiver", "substituição", "remuneração", "resgate", "vencimento antecipado", "alteração", "colateral"]

def log(msg):
    with open(os.path.join(BASE_DIR, "log.txt"), "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} - {msg}\n")
    print(msg)

def detect_event_in_pdf(pdf_bytes):
    try:
        text = extract_text(BytesIO(pdf_bytes)).lower()
        return any(kw in text for kw in KEYWORDS)
    except Exception as e:
        log(f"Erro lendo PDF: {e}")
        return False

def fetch_cvm_agdeb(days=1):
    url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/IPE/DADOS/ipe_agdeb.csv"
    df = pd.read_csv(url, sep=';', encoding='latin1')
    df['DT_REFER'] = pd.to_datetime(df['DT_REFER'], errors='coerce')

    cutoff_date = pd.Timestamp.today() - pd.Timedelta(days=days)
    df_recent = df[df['DT_REFER'] >= cutoff_date]

    results = []
    for _, row in df_recent.iterrows():
        if pd.isna(row['URL_DOC']):
            continue
        try:
            response = requests.get(row['URL_DOC'])
            is_event = detect_event_in_pdf(response.content)
            results.append({
                "emissor": row['DENOM_CIA'],
                "data_documento": row['DT_REFER'].date(),
                "evento_detectado": is_event,
                "link": row['URL_DOC']
            })
        except Exception as e:
            log(f"Erro em {row['DENOM_CIA']}: {e}")
    return results

def fetch_vortx_agds():
    url = "https://vortx.com.br/comunicados"
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")

    results = []
    for link in soup.find_all("a", href=True):
        href = link['href']
        if "ata" in href.lower() or "assembleia" in href.lower():
            try:
                pdf_response = requests.get(href)
                is_event = detect_event_in_pdf(pdf_response.content)
                results.append({
                    "emissor": "Vórtx (diversos)",
                    "data_documento": datetime.date.today(),
                    "evento_detectado": is_event,
                    "link": href
                })
            except:
                continue
    return results

def fetch_pentagono_agds():
    url = "https://www.pentagonotrustee.com.br/informativos"
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")

    results = []
    for link in soup.find_all("a", href=True):
        href = link['href']
        if href.lower().endswith(".pdf") and ("assembleia" in href.lower() or "ata" in href.lower()):
            try:
                full_link = href if href.startswith("http") else "https://www.pentagonotrustee.com.br" + href
                pdf_response = requests.get(full_link)
                is_event = detect_event_in_pdf(pdf_response.content)
                results.append({
                    "emissor": "Pentágono (diversos)",
                    "data_documento": datetime.date.today(),
                    "evento_detectado": is_event,
                    "link": full_link
                })
            except:
                continue
    return results

def fetch_oliveira_trust_agds():
    url = "https://www.olivtrust.com.br/informativos.asp"
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")

    results = []
    for link in soup.find_all("a", href=True):
        href = link['href']
        if ".pdf" in href.lower() and ("assembleia" in href.lower() or "ata" in href.lower()):
            try:
                full_link = href if href.startswith("http") else "https://www.olivtrust.com.br/" + href
                pdf_response = requests.get(full_link)
                is_event = detect_event_in_pdf(pdf_response.content)
                results.append({
                    "emissor": "Oliveira Trust (diversos)",
                    "data_documento": datetime.date.today(),
                    "evento_detectado": is_event,
                    "link": full_link
                })
            except:
                continue
    return results

def save_to_csv(data, label):
    today = datetime.date.today().isoformat()
    filename = os.path.join(BASE_DIR, f"{label}_{today}.csv")
    keys = ["emissor", "data_documento", "evento_detectado", "link"]
    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    log(f"Arquivo salvo: {filename}")

def run_all_sources(days=1):
    log("Iniciando coleta da CVM...")
    cvm_data = fetch_cvm_agdeb(days)
    save_to_csv(cvm_data, "cvm_agdeb")

    log("Iniciando coleta da Vórtx...")
    vortx_data = fetch_vortx_agds()
    save_to_csv(vortx_data, "vortx")

    log("Iniciando coleta da Pentágono...")
    pent_data = fetch_pentagono_agds()
    save_to_csv(pent_data, "pentagono")

    log("Iniciando coleta da Oliveira Trust...")
    ot_data = fetch_oliveira_trust_agds()
    save_to_csv(ot_data, "oliveira_trust")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1, help="Dias anteriores a considerar (default=1)")
    args = parser.parse_args()
    run_all_sources(days=args.days)
