import httpx
import main
import pytest
from test_crud import del_mock_klines, make_mock_klines, add_klines_to_db
import pandas as pd
import io
import xarray as xr
import os
from crud import save_depth

def make_payload_for_xr(symbol: str):
    return dict(
        symbol=symbol,
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


def make_payload():
    return [{
        "symbol": "DOGEUSDT",
        "timeframe": "15m",
        "open_time": 1589436922961,
        "close_time": 1589436922962,
        "open": '0.001',
        "close": '0.002',
        "high": '0.005',
        "low": '0.0001',
        "volume": '22.22',
        "trades": 777,
    },
        {
        "symbol": "DOGEUSDT",
        "timeframe": "15m",
        "open_time": 1589436922964,
        "close_time": 1589436922965,
        "open": '0.001',
        "close": '0.002',
        "high": '0.005',
        "low": '0.0001',
        "volume": '22.22',
        "trades": 888,
    }]


@pytest.mark.asyncio
async def test_add_klines_by_api():
    """Test add klines by API"""
    try:
        payload = make_payload()

        async with httpx.AsyncClient(app=main.app,  base_url="http://test") as client:
            response = await client.post("/klines/", json=payload)

        assert response.status_code == 201
        assert response.json()['added_klines'] == 2
    finally:
        del_mock_klines()

@pytest.mark.asyncio
async def test_get_klines_by_api():

    try:
        pydantic_klines = make_mock_klines()
        add_klines_to_db(pydantic_klines)

        # get all BTCUSDT
        async with httpx.AsyncClient(app=main.app,  base_url="http://test") as client:
            params = {'symbol': 'BTCUSDT', 'timeframe': '15m'}
            response = await client.get("/klines/", params=params)

        assert response.status_code == 200
        assert len(response.json()) == 2

        # get last BTCUSDT
        async with httpx.AsyncClient(app=main.app,  base_url="http://test") as client:
            params = {'symbol': 'BTCUSDT', 'timeframe': '15m', 'limit': 1}
            response = await client.get("/klines/", params=params)

        assert response.status_code == 200
        assert len(response.json()) == 1
        assert response.json()[0][7] == 888

        # get firs BTCUSDT
        async with httpx.AsyncClient(app=main.app,  base_url="http://test") as client:
            params = {'symbol': 'BTCUSDT', 'timeframe': '15m', 'limit_first': 1}
            response = await client.get("/klines/", params=params)

        assert response.status_code == 200
        assert len(response.json()) == 1
        assert response.json()[0][7] == 777

        # get 400 error
        async with httpx.AsyncClient(app=main.app,  base_url="http://test") as client:
            params = {'symbol': 'BTCUSDT', 'timeframe': '15m', 'limit': 1, 'limit_first': 1}
            response = await client.get("/klines/", params=params)

        assert response.status_code == 400
    finally:
        del_mock_klines()

@pytest.mark.asyncio
async def test_get_klines_by_csv():
    """Test getting kline history by csv file"""
    try:
        pydantic_klines = make_mock_klines()
        add_klines_to_db(pydantic_klines)

        async with httpx.AsyncClient(app=main.app, base_url="http://test") as client:
            params = {'symbol': 'BTCUSDT', 'timeframe': '15m'}
            response = await client.get("/klines_csv/", params=params)

            # Проверка статуса ответа
            assert response.status_code == 200

            # Чтение данных файла из ответа
            file_data = io.BytesIO(response.content)

            # Загрузка данных в pandas
            df = pd.read_csv(file_data)

            # Проверка, что массив загружен успешно
            assert len(df) == 2
            assert df.iloc[1]['open_time'] == 1589436922964
    finally:
        del_mock_klines()

@pytest.mark.asyncio
async def test_get_symbols():
    """Test getting symbols list"""
    try:
        pydantic_klines = make_mock_klines()
        add_klines_to_db(pydantic_klines)

        # check all symbols
        async with httpx.AsyncClient(app=main.app, base_url="http://test") as client:
            response = await client.get("/symbols/")

            assert response.status_code == 200
            assert isinstance(response.json(), list)
            assert response.json()[0] == 'BTCUSDT'
            assert len(response.json()) == 2
        
        #check 15m symbols
        async with httpx.AsyncClient(app=main.app, base_url="http://test") as client:
            params = dict(timeframe='15m')
            response = await client.get("/symbols/", params=params)

            assert response.status_code == 200
            assert isinstance(response.json(), list)
            assert len(response.json()) == 1
            assert response.json()[0] == 'BTCUSDT'
    finally:
        del_mock_klines()

@pytest.mark.asyncio
async def test_add_depth() -> None:
    """Test adding depth"""
    symbolname = 'TESTSYMBOL2'
    payload = make_payload_for_xr(symbolname)
    try:
        async with httpx.AsyncClient(app=main.app, base_url="http://test") as client:
            response = await client.post("/depth/", json=payload)

            assert response.status_code == 201
            # Загрузка данных
            with xr.open_dataarray(f'depth/{symbolname}.nc') as array:
                # Проверка, что массив загружен успешно
                assert len(array) == 1
                assert array[0][0][0] == 27406.08
                assert array[0].time == 1234567890
    finally:
        try:
            os.remove(f'depth/{symbolname}.nc')
            pass
        except OSError:
            pass

@pytest.mark.asyncio
async def test_get_depth() -> None:
    """Test getting depth"""
    symbolname = 'TESTSYMBOL3'
    payload = make_payload_for_xr(symbolname)
    try:
        save_depth(payload['symbol'], payload['time'], payload['depth'])
        
        async with httpx.AsyncClient(app=main.app, base_url="http://test") as client:
            response = await client.get("/depth/", params={'symbol': symbolname})

            assert response.status_code == 200
            # Чтение данных файла из ответа
            file_data = io.BytesIO(response.content)

            try:
                with open('temp_output.nc', 'wb') as out_file:
                    out_file.write(file_data.getvalue())

                # Загрузка данных
                with xr.open_dataarray('temp_output.nc') as array:
                    # Проверка, что массив загружен успешно
                    print(array)
                    assert len(array) == 1
                    assert array[0][0][0] == 27406.08
                    assert array[0].time == 1234567890
            finally:
                os.remove('temp_output.nc')
    finally:
        try:
            os.remove(f'depth/{symbolname}.nc')
            pass
        except OSError:
            pass