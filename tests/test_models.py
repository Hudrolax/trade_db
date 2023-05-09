from crud import add_klines_to_db, read_klines
from models import KlinesModel, Klines
from db import SessionLocal
from sqlalchemy.orm import Session
import pytest


def make_mock_klines() -> list[KlinesModel]:
    kline1 = KlinesModel(**{
        "symbol": "DOGEUSDT",
        "timeframe": "15m",
        "open_time": 1589436922959,
        "close_time": 1589436922960,
        "open": 0.001,
        "close": 0.002,
        "high": 0.005,
        "low": 0.0001,
        "volume": 22.22,
        "trades": 777,
    })
    kline2 = KlinesModel(**{
        "symbol": "BTCUSDT",
        "timeframe": "15m",
        "open_time": 1589436922961,
        "close_time": 1589436922962,
        "open": 0.002,
        "close": 0.002,
        "high": 0.005,
        "low": 0.0001,
        "volume": 22.22,
        "trades": 777,
    })
    kline3 = KlinesModel(**{
        "symbol": "BTCUSDT",
        "timeframe": "15m",
        "open_time": 1589436922964,
        "close_time": 1589436922965,
        "open": 0.002,
        "close": 0.002,
        "high": 0.005,
        "low": 0.0001,
        "volume": 22.22,
        "trades": 888,
    })
    return [kline1, kline2, kline3]


def del_mock_klines(db: Session) -> None:
    klines = db.query(Klines).all()
    for kline in klines:
        db.delete(kline)
    db.commit()


@pytest.fixture
def test_db():
    test_session = SessionLocal()
    yield test_session
    test_session.close()


def test_klines_by_crud(test_db):
    """Test reading klines by CRUD function"""
    pydantic_klines = make_mock_klines()
    add_klines_to_db(test_db, pydantic_klines)

    # read BTCUSDT klines
    klines = read_klines(test_db, 'BTCUSDT', '15m')
    assert len(klines) == 2
    assert klines[0].symbol == 'BTCUSDT'

    # read last BTCUSDT kline by limit
    klines = read_klines(test_db, 'BTCUSDT', '15m', limit=1)
    assert len(klines) == 1
    assert klines[0].trades == 888

    # read last BTCUSDT kline by time
    klines = read_klines(test_db, 'BTCUSDT', '15m', start_date=1589436922961)
    assert len(klines) == 1
    assert klines[0].trades == 888

    # read first BTCUSDT kline by time
    klines = read_klines(test_db, 'BTCUSDT', '15m', end_date=1589436922964)
    assert len(klines) == 1
    assert klines[0].trades == 777

    del_mock_klines(test_db)


def test_read_klines_from_db(test_db):
    """Test read klines from db by SQLAlchemy ORM"""

    pydantic_klines = make_mock_klines()
    add_klines_to_db(test_db, pydantic_klines)

    # read kline wich have no in db
    klines = test_db.query(Klines).filter(Klines.symbol == 'WRONG_SYMBOL').all()
    assert len(klines) == 0

    # test read BTCUSDT kline
    klines = test_db.query(Klines).filter(Klines.symbol == 'BTCUSDT').all()
    assert klines[0].open_time == 1589436922961

    # test read all klines
    klines = test_db.query(Klines).filter().all()
    assert len(klines) == 3

    del_mock_klines(test_db)


def test_crud_add_klines_to_db(test_db):
    """Test add klines to DB by CRUD function"""
    klines = make_mock_klines()
    add_klines_to_db(test_db, klines)

    getted_kline1 = test_db.query(Klines).get(
        (klines[0].symbol, klines[0].timeframe, klines[0].open_time))
    getted_kline2 = test_db.query(Klines).get(
        (klines[1].symbol, klines[1].timeframe, klines[1].open_time))
    assert getted_kline1.symbol == 'DOGEUSDT'
    assert getted_kline2.symbol == 'BTCUSDT'

    del_mock_klines(test_db)
