import unittest
from unittest.mock import AsyncMock
from cpapi_client import CPAPIClient


class HistoryEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_uses_supported_read_only_iserver_endpoint(self):
        client = object.__new__(CPAPIClient)
        client._get = AsyncMock(return_value={"data": [{"t": 1, "c": 100}]})
        result = await client.get_market_data_history(123, "1w", "1d", False)
        self.assertEqual([{"t": 1, "c": 100}], result)
        client._get.assert_awaited_once_with(
            "/v1/api/iserver/marketdata/history",
            params={"conid": 123, "period": "1w", "bar": "1d", "outsideRth": "false"},
        )


if __name__ == "__main__":
    unittest.main()
