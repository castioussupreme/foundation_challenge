"""Interacts with NFT chain subgraphs"""
import time
from dataclasses import dataclass
from typing import List
from string import Template
import requests
from requests.adapters import HTTPAdapter, Retry

@dataclass
class Token:
    """Data type for token metadata on chain. Names as exactly as the graphQL server accepts them"""

    name: str
    symbol: str
    totalSupply: int
    volumeUSD: float
    decimals: float


@dataclass
class TokenHourData:
    """Data type for Hourly chain data. Names as exactly as the graphQL server accepts them"""

    periodStartUnix: int
    open: float
    close: float
    high: float
    low: float
    priceUSD: float

class UniswapFetcher:
    """Fetches data from Uniswap

    Attributes
    ----------
    session : requests.Session
        requests session configured to retry and backoff based on response
    """

    UNISWAP_GRAPH_URL = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

    UNISWAP_REQUEST_HEADERS = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36\
            (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
    }

    def __init__(self):
        self.session = requests.Session()

        retries = Retry(
            total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )

        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def fetch_uniswap_token(self, token: str) -> Token:
        """Obtains token data from uniswap

        Args:
            token (str): address of token. eg. 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599

        Returns:
            Token: Metadata for the specified token
        """
        # Prefer templates for readability
        query = Template(
            """
        {
            tokens(
                where: {
                    id: \"${token}\"
                }
            ) 
            {
                ${schema}
            } 
        }
        """
        ).substitute(
            token=token, schema="\n".join([key for key in Token.__annotations__])
        )
        # TODO: determine failure path based on usage
        response = self.session.request(
            "POST",
            UniswapFetcher.UNISWAP_GRAPH_URL,
            headers=UniswapFetcher.UNISWAP_REQUEST_HEADERS,
            json={"query": query},
        ).json()
        if not response or "data" not in response or "tokens" not in response["data"]:
            raise f"Token {token} not found in uniswap"
        return Token(**response["data"]["tokens"][0])

    def fetch_uniswap_hour_datas(
        self,
        token: str,
        start_inclusive_secs: int,
        end_exclusive_secs: int = int(time.time()),
    ) -> List[TokenHourData]:
        """Obtains hour data from uniswap for a specified unix time range. Only returns periods
        with trafic where a transaction was made and thus a price computed. Fetching will be
        performed sequentially as there are unspecified number of entries without data. Number of
        documents per page is 100 for underlying queries.

        Args:
            token (str): address of token. eg. 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599
            start_inclusive_secs (int): start of range to obtain hour data for in unix time
            end_exclusive_secs (int): end of range to obtain hour data for in unix time. Must be 
            less than current time

        Returns:
            List[HourData]: Hourly data for the specified
        """


        if start_inclusive_secs >= end_exclusive_secs:
            raise ValueError("Must be a positive time interval")
        if end_exclusive_secs > int(time.time()):
            raise ValueError("Time bound after current time")

        # Prefer templates for readability
        payload_template = Template(
            """
        {
            tokenHourDatas(
                orderBy: periodStartUnix, 
                where: {
                    token: \"${token}\",
                    periodStartUnix_gte: ${start_inclusive_secs},
                    periodStartUnix_lt: ${end_exclusive_secs},
                    priceUSD_gt: 0
                }
            )
            { 
                ${schema}
            }
        }
        """.strip()
        )

        curr_start = start_inclusive_secs

        fetched_data: List[TokenHourData] = []
        while True:
            curr_start = (
                fetched_data[-1].periodStartUnix + 1
                if fetched_data
                else start_inclusive_secs
            )
            query = payload_template.substitute(
                token=token,
                start_inclusive_secs=curr_start,
                end_exclusive_secs=end_exclusive_secs,
                schema="\n".join([key for key in TokenHourData.__annotations__]),
            )
            # TODO: determine failure path based on usage
            response = self.session.request(
                "POST",
                UniswapFetcher.UNISWAP_GRAPH_URL,
                headers=UniswapFetcher.UNISWAP_REQUEST_HEADERS,
                json={"query": query},
            ).json()
            hour_json_datas = response["data"]["tokenHourDatas"]
            fetched_data.extend(
                [TokenHourData(**hour_json_data) for hour_json_data in hour_json_datas]
            )
            if not hour_json_datas:
                break

        return fetched_data
