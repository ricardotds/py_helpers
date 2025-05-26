#!/usr/bin/env python3
"""
Rotina diária para coletar, processar e resumir Assembleias Gerais de Debenturistas (AGD)
que possam ter criado **eventos corporativos** (ex.: prêmio em D0, waiver‐fee, troca de colateral)
nos últimos *N* dias.

Fontes implementadas:
  • **CVM – Dados Abertos**  (dataset _Cias Abertas: Documentos: Periódicos e Eventuais – IPE_)
  • Lugar‑comum para acrescentar agentes fiduciários (Vórtx, Pentágono, Oliveira Trust).  
    Funções‑stub já estão definidas — basta preencher quando necessário.

Saída:
  • CSV (``output/agd_events_YYYY‑MM‑DD.csv``) com todos os documentos filtrados
  • log.txt para acompanhamento de execução

Uso:
    $ python agd_daily_scraper.py            # busca o dia corrente
    $ python agd_daily_scraper.py --days 5   # última semana

Sugestão de CRON (08h00 São Paulo):
    0 8 * * * /usr/bin/python3 /caminho/agd_daily_scraper.py --days 1 >> /caminho/log.txt 2>&1
"""

from __future__ import annotations
import argparse
import datetime as _dt
import io
import logging
import os
import re
import sys
import zipfile
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from pdfminer.high_level import extract_text

# ────────────────────────────────────────────────────────────────────────────────
# Configurações gerais
# ────────────────────────────────────────────────────────────────────────────────
BASE_URL_CVM = (
    "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{year}.zip"
)
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_PATH = OUTPUT_DIR / "log.txt"

KEYWORDS_EVENTO = [
    r"pr(ê|e)mi[oa]",  # prêmio/remuneração adicional
    r"waiver", r"dispensa", r"dispensar", r"renunci[ae]",  # waiver‑fee/renúncia
    r"substitui[çc][aã]o",  # troca de garantia / ativo / contrato
    r"amortiza(ç|c)[aã]o",  # amortização extraordinária
    r"resgate", r"vencimento antecipado",  # resgate / venc. antecipado
]
RE_EVENTO = re.compile("|".join(KEYWORDS_EVENTO), re.IGNORECASE)

# ────────────────────────────────────────────────────────────────────────────────
# Auxiliares
# ────────────────────────────────────────────────────────────────────────────────

def _setup_logger(debug: bool = False):
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filemode="a",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if debug else logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logging.getLogger().addHandler(console)


def _today_sp() -> _dt.date:
    """Data atual em America/Sao_Paulo sem depender de pytz."""
    return _dt.datetime.utcnow().astimezone().date()  # ajustar se usar zoneinfo


def _cutoff(days: int) -> _dt.date:
    return _today_sp() - _dt.timedelta(days=days)


# ────────────────────────────────────────────────────────────────────────────────
# Download + parsing CVM IPE
# ────────────────────────────────────────────────────────────────────────────────

def fetch_cvm_agdeb(days: int) -> pd.DataFrame:
    """Baixa o ZIP do ano corrente, filtra por AGD e janela de dias."""

    year = _today_sp().year
    url = BASE_URL_CVM.format(year=year)
    logging.info("Baixando dataset IPE %s", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        # tenta localizar o primeiro .csv/.txt dentro do zip
        inner = next((n for n in zf.namelist() if n.lower().endswith(('.csv', '.txt'))))
        logging.debug("Arquivo interno encontrado: %s", inner)
        with zf.open(inner) as data_file:
            # os arquivos são pipe '|' ou ';' — tenta auto‑detectar
            sample = data_file.read(2048).decode("latin1", errors="ignore")
            sep = '|' if '|' in sample.splitlines()[0] else ';'
        df = pd.read_csv(
            zf.open(inner),
            sep=sep,
            encoding="latin1",
            low_memory=False,
        )

    # Normaliza colunas para caixa alta sem acentos
    df.columns = (
        df.columns.str.normalize("NFKD").str.encode("ascii", errors="ignore").str.decode()
    )

    # Colunas principais esperadas
    col_date = "DT_ENTREGA"
    col_categoria = "CATEGORIA"
    col_tipo = "TP_DOC"

    # Ajuste para colunas alternativas
    for alt in ["DT_REFER", "DT_REF", "DT_ARQUIVO"]:
        if alt in df.columns:
            col_date = alt
            break
    for alt in ["CATEGORIA_DOCUMENTO", "CAT" , "CATEGORIA_DOC"]:
        if alt in df.columns:
            col_categoria = alt
            break
    for alt in ["DS_TIPO", "TP_ASSEMBLEIA", "TIPO"]:
        if alt in df.columns:
            col_tipo = alt
            break

    # Converte datas
    df[col_date] = pd.to_datetime(df[col_date], errors="coerce").dt.date

    cutoff = _cutoff(days)
    mask = (
        (df[col_categoria].str.upper() == "ASSEMBLEIA")
        & df[col_tipo].str.contains("AGDEB", case=False, na=False)
        & (df[col_date] >= cutoff)
    )

    filtered = df.loc[mask].copy()
    if filtered.empty:
        logging.info("Nenhum documento AGDEB encontrado na janela de %d dia(s).", days)
        return filtered

    logging.info("%d documentos AGDEB encontrados.", len(filtered))

    # garante existência de coluna URL para download do PDF
    if "URL" not in filtered.columns:
        logging.warning("Coluna URL não localizada — PDFs não serão baixados.")
        filtered["FLAG_EVENTO"] = None
        return filtered

    # Baixa e analisa PDFs
    eventos: List[bool] = []
    for url in filtered["URL"]:
        try:
            pdf_resp = requests.get(url, timeout=60)
            pdf_resp.raise_for_status()
            text = extract_text(io.BytesIO(pdf_resp.content))
            eventos.append(bool(RE_EVENTO.search(text)))
        except Exception as e:
            logging.error("Falha ao processar %s: %s", url, e)
            eventos.append(None)

    filtered["FLAG_EVENTO"] = eventos
    return filtered


# ────────────────────────────────────────────────────────────────────────────────
# Stubs para agentes fiduciários — preencha conforme necessidade
# ────────────────────────────────────────────────────────────────────────────────

def fetch_vortx(days: int) -> pd.DataFrame:  # pragma: no cover
    """Exemplo de stub — implementar scraping do portal Vórtx."""
    logging.info("[stub] fetch_vortx ainda não implementado.")
    return pd.DataFrame()


def fetch_pentagono(days: int) -> pd.DataFrame:  # pragma: no cover
    logging.info("[stub] fetch_pentagono ainda não implementado.")
    return pd.DataFrame()


def fetch_oliveira_trust(days: int) -> pd.DataFrame:  # pragma: no cover
    logging.info("[stub] fetch_oliveira_trust ainda não implementado.")
    return pd.DataFrame()


# ────────────────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Busca diária de AGDs (AGDEB)")
    parser.add_argument("--days", type=int, default=1, help="Número de dias a voltar (default=1)")
    parser.add_argument("--debug", action="store_true", help="Modo de depuração verboso")
    args = parser.parse_args()

    _setup_logger(args.debug)
    logging.info("Iniciando rotina AGD para os últimos %d dias", args.days)

    dfs: List[pd.DataFrame] = []

    try:
        dfs.append(fetch_cvm_agdeb(args.days))
        dfs.append(fetch_vortx(args.days))
        dfs.append(fetch_pentagono(args.days))
        dfs.append(fetch_oliveira_trust(args.days))
    except Exception as exc:
        logging.exception("Erro inesperado: %s", exc)
        sys.exit(1)

    df_final = pd.concat(dfs, ignore_index=True, sort=False).drop_duplicates()

    if df_final.empty:
        logging.info("Nenhum resultado encontrado em nenhuma fonte.")
        return

    today_str = _today_sp().isoformat()
    out_path = OUTPUT_DIR / f"agd_events_{today_str}.csv"
    df_final.to_csv(out_path, index=False, encoding="utf-8-sig")
    logging.info("Arquivo salvo em %s (%d linhas)", out_path, len(df_final))


if __name__ == "__main__":
    main()
