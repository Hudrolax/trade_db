import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse
from starlette.responses import StreamingResponse
from tempfile import NamedTemporaryFile
from sqlalchemy.orm import Session
import models
from db import engine, get_db
from crud import add_klines_to_db, read_klines, read_symbols
from typing import List
import numpy as np
import h5py
import os

# Create tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI()


@app.post("/klines/", status_code=201)
async def add_klines(
    klines: List[models.KlinesModel],
    db: Session = Depends(get_db),
) -> dict[str, int]:
    added_klines = add_klines_to_db(db, klines)
    return {"added_klines": len(added_klines)}


@app.get("/klines/")
async def get_klines(
        symbol: str,
        timeframe: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int | None = None,
        db: Session = Depends(get_db),
        limit_first: int | None = None,
):
    if limit is not None and limit_first is not None:
        raise HTTPException(
            status_code=400, detail="limit and limit_first params cannot be sent at the same time")

    if limit is None and limit_first is None:
        limit = 500

    klines = read_klines(
        db=db,
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        limit_first=limit_first,
    )
    result = []
    for kline in klines:
        result.append([
            kline.open_time,
            kline.close_time,
            kline.open,
            kline.high,
            kline.low,
            kline.close,
            kline.volume,
            kline.trades,
        ])

    return result


@app.get("/klines_h5/")
async def get_file(
        symbol: str,
        timeframe: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int | None = None,
        db: Session = Depends(get_db),
        limit_first: int | None = None,
):
    klines = read_klines(
        db=db,
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        limit_first=limit_first,
    )
    result = []
    for kline in klines:
        result.append([
            kline.open_time,
            kline.close_time,
            kline.open,
            kline.high,
            kline.low,
            kline.close,
            kline.volume,
            kline.trades,
        ])
    nparray = np.array(result)

    # Создаем временный файл
    tmp_file = NamedTemporaryFile(delete=False, suffix=".h5", dir="/tmp")
    tmp_file.close()

    # Сохраняем nparray в файл h5
    with h5py.File(tmp_file.name, 'w') as hf:
        hf.create_dataset("nparray",  data=nparray)

    # Возвращаем файл в ответ на GET-запрос
    def file_generator():
        with open(tmp_file.name, "rb") as f:
            yield from f

        # Удаляем временный файл
        os.remove(tmp_file.name)

    response = StreamingResponse(
        file_generator(), media_type='application/octet-stream')

    return response


@app.get("/symbols/")
async def get_symbols(db: Session = Depends(get_db), timeframe: str | None = None):
    return read_symbols(db, timeframe)


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
