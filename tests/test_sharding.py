import unittest

from fastapi import HTTPException

from shard_map import SHARD_MAP, shard_for_account


class ShardMapTests(unittest.TestCase):
    def test_map_config_contains_the_two_authoritative_ranges(self) -> None:
        self.assertEqual(
            [(shard.shard_id, shard.range_start, shard.range_end) for shard in SHARD_MAP],
            [
                ("shard-a", "A0001", "A1000"),
                ("shard-b", "A1001", "A2000"),
            ],
        )

    def test_routes_account_ranges_to_distinct_shards(self) -> None:
        self.assertEqual(shard_for_account("A0001"), "shard-a")
        self.assertEqual(shard_for_account("A1000"), "shard-a")
        self.assertEqual(shard_for_account("A1001"), "shard-b")
        self.assertEqual(shard_for_account("A2000"), "shard-b")

    def test_rejects_account_ids_outside_shard_ranges(self) -> None:
        for account in ("A0000", "A2001", "A1", "a1001", "B1001"):
            with self.subTest(account=account):
                with self.assertRaises(HTTPException) as error:
                    shard_for_account(account)
                self.assertEqual(error.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
