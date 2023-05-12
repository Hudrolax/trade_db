from sqlalchemy.orm import Session
from sqlalchemy import distinct
import models


def add_klines_to_db(db: Session, klines: list[models.KlinesModel]) -> list[models.Klines]:
    result = []
    for item in klines:
        kline = models.Klines(**item.dict())
        db.merge(kline)
        result.append(kline)
    db.commit()
    return result

def read_symbols(db: Session, timeframe: str | None):
    """Function read and return list of unique symbols in DB"""
    query = db.query(distinct(models.Klines.symbol))
    if timeframe is not None:
        query = query.filter(models.Klines.timeframe == timeframe)
    return sorted([row[0] for row in query.all()])

def read_klines(
        db: Session,
        symbol: str,
        timeframe: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int | None = None,
        limit_first: int | None =  None,
) -> list[models.Klines]:
    query = db.query(models.Klines).filter(models.Klines.symbol == symbol,
                                           models.Klines.timeframe == timeframe)

    if start_date is not None:
        query = query.filter(models.Klines.open_time > start_date)

    if end_date is not None:
        query = query.filter(models.Klines.open_time < end_date)

    # Order by open_time
    if limit is not None:
        query = query.order_by(models.Klines.open_time.desc())
    elif limit_first is not None:
        query = query.order_by(models.Klines.open_time.asc())

    # Apply limit if it's not None
    if limit is not None:
        query = query.limit(limit)
    elif limit_first is not None:
        query = query.limit(limit_first)

    records = query.all()
    sorted_records = sorted(records, key=lambda r: r.open_time)

    return sorted_records
