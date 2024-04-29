"""Tests for the subgraph"""
import unittest
import requests_mock

from subgraph_demo import subgraph

class TestUniswapFetcher(unittest.TestCase):
    """Tests for the Uniswap Fetcher"""

    def setUp(self):
        self.uniswap_fetcher = subgraph.UniswapFetcher()

    def test_fetch_uniswap_hour_datas_positive_range_value_error(self):
        """time ranges must be positive"""
        with self.assertRaises(ValueError):
            self.uniswap_fetcher.fetch_uniswap_hour_datas("test_token", 2, 1)

    def test_fetch_uniswap_hour_datas_current_time_value_error(self):
        """time ranges must be up to present"""
        with self.assertRaises(ValueError):
            self.uniswap_fetcher.fetch_uniswap_hour_datas(
                "test_token", 99999999999999, 99999999999999)

    @requests_mock.Mocker()
    def test_fetch_uniswap_hour_datas_pagination(self, request_mock):
        """ensure pagination doesn't repeat previous results and fetches correctly"""
        token = "test_token"
        start_time = 100
        end_time = 200

        # Define a dynamic response based on the request parameters
        def dynamic_response(request, _):
            if b'periodStartUnix_gte: ' + str.encode(str(start_time)) in request.body:
                return {
                    "data": {
                        "tokenHourDatas": [{
                            "periodStartUnix": start_time,
                            "open": 100.0,
                            "close": 120.0,
                            "high": 130.0,
                            "low": 90.0,
                            "priceUSD": 110.0
                        }],
                    }
                }
            if b'periodStartUnix_gte: ' + str.encode(str(start_time + 1)) in request.body:
                return {
                    "data": {
                        "tokenHourDatas": [{
                            "periodStartUnix": end_time,
                            "open": 120.0,
                            "close": 140.0,
                            "high": 150.0,
                            "low": 110.0,
                            "priceUSD": 130.0
                        }],
                    }
                }
            return {
                "data": {
                    "tokenHourDatas": []
                }
            }

        request_mock.post(subgraph.UniswapFetcher.UNISWAP_GRAPH_URL, json=dynamic_response)

        hour_data = self.uniswap_fetcher.fetch_uniswap_hour_datas(token, start_time, end_time)

        expected_hour_data = [
            subgraph.TokenHourData(
                periodStartUnix=start_time,
                open=100.0,
                close=120.0,
                high=130.0,
                low=90.0,
                priceUSD=110.0
            ),
            subgraph.TokenHourData(
                periodStartUnix=end_time,
                open=120.0,
                close=140.0,
                high=150.0,
                low=110.0,
                priceUSD=130.0
            )
        ]

        self.assertEqual(hour_data, expected_hour_data)

if __name__ == "__main__":
    unittest.main()
