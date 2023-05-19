import httpx
import main
import pytest
from test_crud import del_mock_klines, make_mock_klines, add_klines_to_db
import pandas as pd
import io


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
