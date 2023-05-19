import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse
from starlette.responses import StreamingResponse
from tempfile import NamedTemporaryFile
import models
from crud import add_klines_to_db, read_klines, read_symbols
from typing import List
import numpy as np
import os

file_locks = {}
app = FastAPI()


@app.post("/klines/", status_code=201)
async def add_klines(
    klines: List[models.KlinesModel],
) -> dict[str, int]:
    added_klines = add_klines_to_db(klines)
    return {"added_klines": added_klines}


@app.get("/klines/")
async def get_klines(
        symbol: str,
        timeframe: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int | None = None,
        limit_first: int | None = None,
):
    if limit is not None and limit_first is not None:
        raise HTTPException(
            status_code=400, detail="limit and limit_first params cannot be sent at the same time")

    if limit is None and limit_first is None:
        limit = 500

    klines = read_klines(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        limit_first=limit_first,
    )

    return klines.values.tolist()


@app.get("/klines_csv/")
async def get_file(
        symbol: str,
        timeframe: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int | None = None,
        limit_first: int | None = None,
):
    klines = read_klines(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        limit_first=limit_first,
    )

    # Создаем временный файл
    tmp_file = NamedTemporaryFile(delete=False, suffix=".csv", dir="/tmp")
    tmp_file.close()

    # Сохраняем dataframe в файл csv
    klines.to_csv(tmp_file.name, index=False)

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
async def get_symbols(timeframe: str | None = None):
    return read_symbols(timeframe)


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
