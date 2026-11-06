import argparse
import unittest
from unittest.mock import MagicMock, patch

import httpx

from client.client import _gateway_url, _send


class BankingClientTests(unittest.TestCase):
    def test_gateway_url_requires_a_single_gateway(self) -> None:
        self.assertEqual(
            _gateway_url(" http://gateway:8000/ "),
            "http://gateway:8000",
        )
        with self.assertRaises(argparse.ArgumentTypeError):
            _gateway_url("http://gateway-a:8000,http://gateway-b:8000")

    def test_requests_are_sent_to_the_configured_gateway(self) -> None:
        response = httpx.Response(
            200,
            request=httpx.Request("GET", "http://gateway:8000/balance/inquiry"),
        )
        client = MagicMock()
        client.__enter__.return_value = client
        client.request.return_value = response

        with patch("client.client.httpx.Client", return_value=client):
            result = _send(
                "http://gateway:8000",
                "GET",
                "/balance/inquiry",
                params={"account": "A0001"},
            )

        self.assertIs(result, response)
        client.request.assert_called_once_with(
            "GET",
            "http://gateway:8000/balance/inquiry",
            json=None,
            params={"account": "A0001"},
            headers={},
        )


if __name__ == "__main__":
    unittest.main()
