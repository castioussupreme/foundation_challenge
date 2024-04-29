"""Backfills and initializes service to serve subgraph data"""

import threading
import time
import uvicorn
from fastapi import FastAPI

from subgraph_demo.subgraph import UniswapFetcher
from subgraph_demo.subgraph_dao import SubgraphDAO, Token, TokenHourData

TOKENS = {
    "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
    "SHIB": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
    "GNO": "0x6810e776880c02933d47db1b9fc05908e5386b96",
}

SEVEN_DAY_SECONDS = 604800
HOUR_SECONDS = 3600

# Used to signal API service start as well as to continue polling
SERVICE_BARRIER = threading.Barrier(len(TOKENS) + 1)


# SERVICE DEFINITION

app = FastAPI()
subgraph_dao = SubgraphDAO()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/getChartData/{token_symbol}")
async def get_chart_data(token_symbol: str, time_unit_hours: int = 1):
    return subgraph_dao.get_token_hour_data(token_symbol.upper())


class UniswapDataCollector:
    """Collects subgraph data"""

    def __init__(self, token_address: str):
        self.token_address = token_address
        self.uniswap_fetcher = UniswapFetcher()
        self.subgraph_dao = SubgraphDAO()
        # TODO: determine if this needs to be polled again after some time
        metadata = self.uniswap_fetcher.fetch_uniswap_token(token_address)

        self.symbol = metadata.symbol
        self.subgraph_dao.upsert_token_metadata(
            Token(
                metadata.name,
                metadata.symbol,
                metadata.totalSupply,
                metadata.volumeUSD,
                metadata.decimals,
            )
        )
        # TODO get last available time from DB and store locally here for next poll

    def fetch_subgraph_data(self, start_time_secs: int) -> int:
        """Fetches subgraph data from the specified time

        Args:
            start_time_secs (int): furthest time back to backfill

        Returns:
            int: latest fetched start time, start_time_secs if no data was fetched
        """
        current_time = int(time.time())
        new_hour_data = self.uniswap_fetcher.fetch_uniswap_hour_datas(
            self.token_address, start_time_secs, current_time
        )
        db_new_hour_data = [
            TokenHourData(
                token_symbol=self.symbol,
                period_start_unix=nhd.periodStartUnix,
                open=nhd.open,
                close=nhd.close,
                high=nhd.high,
                low=nhd.low,
                price_usd=nhd.priceUSD,
            )
            for nhd in new_hour_data
        ]
        self.subgraph_dao.upsert_token_hour_data_batch(db_new_hour_data)
        return new_hour_data[-1].periodStartUnix if new_hour_data else start_time_secs

    def poll_hourly_data(self, start_time_secs: int) -> None:
        """Fetches hourly data on a polling basis, first fetching all data from
        start_time_secs and then new data

        Args:
            start_time_secs (int): time from which to backfill from and keep polling thereafter
        """
        new_start_time_secs = start_time_secs
        while True:
            new_start_time_secs = self.fetch_subgraph_data(new_start_time_secs)
            # TODO: consider using new_start_time_secs plus an hour and change if there
            # is consistency for data freshness
            time.sleep(HOUR_SECONDS)


def token_data_collect_task(token_name: str, token_address: str) -> None:
    """Backfills subgraph data and waits for other backfills to finish prior to polling
    from last obtained timestamp

    Args:
        name (str): human readable crypto name for debugging
        address (str): used to backfill and poll for subgraph data
    """
    collector = UniswapDataCollector(token_address)
    current_time = int(time.time())
    latest_obtained_time = collector.fetch_subgraph_data(
        current_time - SEVEN_DAY_SECONDS
    )
    print(f"Done backfilling ${token_name}...")
    # Hold off on polling to ensure data freshness across API calls is obtained
    SERVICE_BARRIER.wait()
    collector.poll_hourly_data(latest_obtained_time)


if __name__ == "__main__":
    for name, address in TOKENS.items():
        t = threading.Thread(
            target=token_data_collect_task,
            args=(
                name,
                address,
            ),
        )
        t.start()
    SERVICE_BARRIER.wait()
    print("🚀 Backfill complete - starting API service and continuing to poll 🚀")

    uvicorn.run(
        "subgraph_demo.orchestrator:app", host="127.0.0.1", port=8000, reload=True
    )
