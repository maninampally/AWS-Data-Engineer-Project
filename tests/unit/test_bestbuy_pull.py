import datetime as dt
from unittest import mock

from etl.bronze.bestbuy_pull import build_s3_key


def test_build_s3_key_uses_today_date():
    fake_date = dt.date(2025, 1, 2)

    with mock.patch("etl.bronze.bestbuy_pull.dt") as mock_dt:
        mock_dt.date.today.return_value = fake_date
        mock_dt.date.strftime = dt.date.strftime  # use real strftime

        key = build_s3_key("bestbuy/catalog")

    assert "dt=2025-01-02" in key
    assert key.startswith("bestbuy/catalog/dt=2025-01-02/")
    assert key.endswith("bestbuy_products_2025-01-02.jsonl")
