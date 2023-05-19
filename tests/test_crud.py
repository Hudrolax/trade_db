from crud import add_klines_to_db, read_klines, delete_directory
from models import KlinesModel
import pandas as pd


def make_mock_klines() -> list[KlinesModel]:
    kline1 = KlinesModel(**{
        "symbol": "DOGEUSDT",
        "timeframe": "1m",
        "open_time": 1589436922959,
        "close_time": 1589436922960,
        "open": '0.001',
        "close": '0.002',
        "high": '0.005',
        "low": '0.0001',
        "volume": '22.22',
        "trades": 777,
    })
    kline2 = KlinesModel(**{
        "symbol": "BTCUSDT",
        "timeframe": "15m",
        "open_time": 1589436922961,
        "close_time": 1589436922962,
        "open": '0.002',
        "close": '0.002',
        "high": '0.005',
        "low": '0.0001',
        "volume": '22.22',
        "trades": 777,
    })
    kline3 = KlinesModel(**{
        "symbol": "BTCUSDT",
        "timeframe": "15m",
        "open_time": 1589436922964,
        "close_time": 1589436922965,
        "open": '0.002',
        "close": '0.002',
        "high": '0.005',
        "low": '0.0001',
        "volume": '22.22',
        "trades": 888,
    })
    return [kline1, kline2, kline3]


def del_mock_klines():
    delete_directory()


def test_crud_read_write():
    """Test add klines to DB by CRUD function"""
    try:
        klines = make_mock_klines()
        add_klines_to_db(klines)

        result = read_klines(symbol='BTCUSDT', timeframe='15m')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert result.iloc[0]['open_time'] ==1589436922961 

        result = read_klines(symbol='DOGEUSDT', timeframe='15m')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

        result = read_klines(symbol='DOGEUSDT', timeframe='1m')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    finally:
        del_mock_klines()
