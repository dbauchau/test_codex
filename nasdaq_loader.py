"""Script pour charger les cours des actions du NASDAQ dans une base SQLite.

Le script télécharge la liste des symboles du NASDAQ depuis le site officiel,
utilise yfinance pour récupérer les cours journaliers et enregistre le tout
dans une base de données SQLite. Chaque exécution ajoute uniquement les
nouveaux cours qui ne sont pas déjà présents dans la base.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import logging
import sqlite3
from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd
import requests
import yfinance as yf


NASDAQLISTED_URL = "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
DEFAULT_START_DATE = dt.date(2010, 1, 1)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Charge les cours journaliers de toutes les actions du NASDAQ dans "
            "une base SQLite. Lors d'une exécution ultérieure, seuls les "
            "nouveaux cours sont ajoutés."
        )
    )
    parser.add_argument(
        "--database",
        default="nasdaq_prices.db",
        help="Chemin vers le fichier SQLite où stocker les données.",
    )
    parser.add_argument(
        "--start-date",
        help=(
            "Date de début au format AAAA-MM-JJ. Si non précisée, la date est "
            "déduite automatiquement à partir des données déjà enregistrées."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Nombre maximal de symboles à traiter (utile pour des tests).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Niveau de log (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER,
            PRIMARY KEY (symbol, date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )


def get_metadata(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cursor = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else None


def set_metadata(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def fetch_nasdaq_symbols(limit: Optional[int] = None) -> List[str]:
    logging.info("Téléchargement de la liste des symboles NASDAQ...")
    response = requests.get(NASDAQLISTED_URL, timeout=30)
    response.raise_for_status()

    # Le fichier est séparé par | et contient une dernière ligne descriptive
    buffer = io.StringIO(response.text)
    df = pd.read_csv(buffer, sep="|")
    df = df[df["Symbol"].notna()]
    df = df[~df["Symbol"].str.contains("File Creation Time", na=False)]
    symbols = df["Symbol"].tolist()

    if limit is not None:
        symbols = symbols[:limit]

    logging.info("%d symboles récupérés", len(symbols))
    return symbols


def get_last_date_for_symbol(conn: sqlite3.Connection, symbol: str) -> Optional[dt.date]:
    cursor = conn.execute(
        "SELECT date FROM prices WHERE symbol = ? ORDER BY date DESC LIMIT 1",
        (symbol,),
    )
    row = cursor.fetchone()
    return dt.date.fromisoformat(row[0]) if row else None


def determine_start_date(
    conn: sqlite3.Connection, user_start: Optional[str],
) -> dt.date:
    if user_start:
        return dt.date.fromisoformat(user_start)

    last_sync = get_metadata(conn, "last_sync_date")
    if last_sync:
        return dt.date.fromisoformat(last_sync) + dt.timedelta(days=1)

    return DEFAULT_START_DATE


@dataclass
class DownloadResult:
    symbol: str
    rows: List[tuple]
    max_date: Optional[dt.date]


def download_prices_for_symbol(
    symbol: str, start_date: dt.date, end_date: dt.date
) -> DownloadResult:
    if start_date > end_date:
        return DownloadResult(symbol, [], None)

    logging.debug(
        "Téléchargement des données pour %s entre %s et %s",
        symbol,
        start_date,
        end_date,
    )
    # yfinance considère la date de fin comme exclusive.
    yf_end = end_date + dt.timedelta(days=1)
    data = yf.download(
        symbol,
        start=start_date.isoformat(),
        end=yf_end.isoformat(),
        progress=False,
        auto_adjust=False,
        actions=False,
        threads=False,
    )

    if data.empty:
        logging.debug("Aucune donnée renvoyée pour %s", symbol)
        return DownloadResult(symbol, [], None)

    data = data.rename(columns=str.lower)
    data.index = pd.to_datetime(data.index)
    rows: List[tuple] = []
    max_date: Optional[dt.date] = None
    for index, values in data.iterrows():
        day = index.date()
        max_date = max(max_date, day) if max_date else day
        rows.append(
            (
                symbol,
                day.isoformat(),
                float(values.get("open", float("nan"))),
                float(values.get("high", float("nan"))),
                float(values.get("low", float("nan"))),
                float(values.get("close", float("nan"))),
                float(values.get("adj close", values.get("adj_close", float("nan")))),
                int(values.get("volume", 0)) if not pd.isna(values.get("volume")) else None,
            )
        )

    return DownloadResult(symbol, rows, max_date)


def insert_rows(conn: sqlite3.Connection, rows: Iterable[tuple]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    conn.executemany(
        """
        INSERT OR IGNORE INTO prices (
            symbol, date, open, high, low, close, adj_close, volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return conn.total_changes


def main() -> None:
    args = parse_arguments()
    configure_logging(args.log_level)

    end_date = dt.date.today()
    logging.info("Date de fin utilisée : %s", end_date)

    with sqlite3.connect(args.database) as conn:
        ensure_schema(conn)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        global_start = determine_start_date(conn, args.start_date)
        logging.info("Date de début globale : %s", global_start)

        symbols = fetch_nasdaq_symbols(limit=args.limit)

        total_inserted = 0
        latest_date: Optional[dt.date] = None

        for idx, symbol in enumerate(symbols, start=1):
            last_date = get_last_date_for_symbol(conn, symbol)
            start_date = max(global_start, last_date + dt.timedelta(days=1)) if last_date else global_start

            if start_date > end_date:
                logging.debug(
                    "Symbol %s déjà à jour (dernière date %s)", symbol, last_date
                )
                continue

            try:
                result = download_prices_for_symbol(symbol, start_date, end_date)
            except Exception as exc:  # noqa: BLE001 - journaliser les erreurs réseau/API
                logging.warning("Échec du téléchargement pour %s : %s", symbol, exc)
                continue

            inserted = insert_rows(conn, result.rows)
            conn.commit()
            total_inserted += inserted

            if result.max_date and (latest_date is None or result.max_date > latest_date):
                latest_date = result.max_date

            logging.info(
                "[%d/%d] %s : %d nouvelles lignes",
                idx,
                len(symbols),
                symbol,
                inserted,
            )

        if latest_date:
            set_metadata(conn, "last_sync_date", latest_date.isoformat())
            conn.commit()

    logging.info("Import terminé : %d nouvelles lignes insérées", total_inserted)


if __name__ == "__main__":
    main()
