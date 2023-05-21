from crud import add_klines_to_db, read_klines, delete_directory, save_depth, read_depth
from models import KlinesModel
import pandas as pd
import xarray as xr
import os


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
        assert result.iloc[0]['open_time'] == 1589436922961

        result = read_klines(symbol='DOGEUSDT', timeframe='15m')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

        result = read_klines(symbol='DOGEUSDT', timeframe='1m')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
    finally:
        del_mock_klines()


def test_crud_add_depth():
    payload = dict(
        symbol='TESTSYMBOL',
        time=1234567890,
        depth=[
            [27406.08, 135.33290100097656],
            [27369.601, 30.44615936279297],
            [27333.122, 69.48698425292969],
            [27296.643, 10.547340393066406],
            [27260.164, 47.64459991455078],
            [27223.685, 42.6150016784668],
            [27187.205, 20.082590103149414],
            [27150.726, 45.36573028564453],
            [27114.247, 73.5325698852539],
            [27077.768, 80.94225311279297],
            [27041.279, 48.289772033691406],
            [27001.438, 41.00360107421875],
            [26961.597, 39.912052154541016],
            [26921.756, 36.83224105834961],
            [26881.915, 98.67887115478516],
            [26842.074, 102.25617980957031],
            [26802.233, 126.08503723144531],
            [26762.392, 72.70709991455078],
            [26722.551, 133.31883239746094],
            [26682.71, 165.39324951171875],
        ]
    )
    try:
        save_depth(symbol=payload['symbol'], time=payload['time'], depth=payload['depth'])
        da = read_depth(symbol=payload['symbol'])
        assert isinstance(da, xr.DataArray)
        assert len(da) == 1
        assert len(da[0]) == 20
        assert da[0].time == 1234567890
        assert da[0][0][0] == 27406.08
        assert da[0][0][1] == 135.33290100097656
    finally:
        try:
            os.remove('depth/TESTSYMBOL.nc')
        except OSError:
            pass
