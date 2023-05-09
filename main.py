import uvicorn
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import models
from db import engine, get_db
from crud import add_klines_to_db, read_klines
from typing import List

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
async def read_klines(
        symbol: str,
        timeframe: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int | None = None,
        db: Session = Depends(get_db),
):
    result = read_klines(
        db=db,
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    return result

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
