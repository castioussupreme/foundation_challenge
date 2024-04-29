"""Used for interactions with the database layer"""

import sqlite3
from dataclasses import dataclass
from typing import List


@dataclass
class Token:
    """Data type for token metadata on chain. Names as exactly as the graphQL server accepts them"""

    name: str
    symbol: str
    total_supply: int
    volume_usd: float
    decimals: float


@dataclass
class TokenHourData:
    """Data type for Hourly chain data. Names as exactly as the graphQL server accepts them"""

    token_symbol: str
    period_start_unix: int
    open: float
    close: float
    high: float
    low: float
    price_usd: float


class SubgraphDAO:
    """Interacts with subgraph database"""

    SUBGRAPH_DB_NAME = "subgraph.db"

    def __init__(self, db_name=SUBGRAPH_DB_NAME):
        self.conn = sqlite3.connect(db_name)
        self.create_tables()

    def __enter__(self, db_name=SUBGRAPH_DB_NAME):
        self.conn = sqlite3.connect(db_name)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def create_tables(self):
        cursor = self.conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS token_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT UNIQUE,
                name TEXT,
                total_supply INTEGER,
                volume_usd REAL,
                decimals REAL
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS token_hour_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_symbol TEXT,
                period_start_unix INTEGER,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                price_usd REAL,
                UNIQUE(token_symbol, period_start_unix)
            )
        """
        )
        self.conn.commit()

    def upsert_token_metadata(self, token: Token):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO token_metadata (symbol, name, total_supply, volume_usd, decimals)
            VALUES (?, ?, ?, ?, ?)
        """,
            (
                token.symbol,
                token.name,
                token.total_supply,
                token.volume_usd,
                token.decimals,
            ),
        )
        self.conn.commit()

    def upsert_token_hour_data_batch(self, token_hour_data_list: List[TokenHourData]):
        cursor = self.conn.cursor()
        cursor.executemany(
            """
            INSERT OR REPLACE INTO token_hour_data (
                token_symbol, period_start_unix, open, close, high, low, price_usd)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            [
                (
                    thd.token_symbol,
                    thd.period_start_unix,
                    thd.open,
                    thd.close,
                    thd.high,
                    thd.low,
                    thd.price_usd,
                )
                for thd in token_hour_data_list
            ],
        )
        self.conn.commit()

    def get_token_hour_data(self, token_symbol: str) -> List[TokenHourData]:
        """Retrieve token hour data for a given token symbol"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM token_hour_data WHERE token_symbol=? ORDER BY period_start_unix ASC",
            (token_symbol,),
        )
        rows = cursor.fetchall()

        token_hour_data_list = []
        for row in rows:
            token_hour_data = TokenHourData(
                token_symbol=row[1],
                period_start_unix=row[2],
                open=row[3],
                close=row[4],
                high=row[5],
                low=row[6],
                price_usd=row[7],
            )
            token_hour_data_list.append(token_hour_data)

        return token_hour_data_list

    def get_token_metadata(self, token_symbol: str) -> Token:
        """Retrieve token metadata for a given token symbol"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM token_metadata WHERE symbol=?",
            (token_symbol,),
        )
        row = cursor.fetchone()

        if row:
            token = Token(
                name=row[2],
                symbol=row[1],
                total_supply=row[3],
                volume_usd=row[4],
                decimals=row[5],
            )
            return token
        else:
            return None

    def close(self):
        """Closes connection on exit"""
        self.conn.close()
