from models import KlinesModel
import pandas as pd
import os


col_types = dict(
    open_time='int64',
    close_time='int64',
    open=object,
    high=object,
    low=object,
    close=object,
    volume=object,
    trades='int64',
)


def get_filename(symbol: str, tf: str):
    return f'klines/{symbol}_{tf}.csv'


def delete_directory(directory_path: str = 'klines'):
    # Удаляем все файлы и поддиректории
    try:
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            if os.path.isfile(item_path):
                os.remove(item_path)  # Удаляем файл
            elif os.path.isdir(item_path):
                delete_directory(item_path)  # Рекурсивно удаляем директорию
        # Удаляем саму директорию
        os.rmdir(directory_path)
    except FileNotFoundError:
        pass


def convert_to_dataframe(klines: list[KlinesModel]) -> pd.DataFrame:
    klines_dict = [kline.dict() for kline in klines]
    df = pd.DataFrame(klines_dict)
    return df


def dataframe_to_klines(df: pd.DataFrame) -> list[KlinesModel]:
    return [KlinesModel(**kwargs) for kwargs in df.to_dict(orient='records')]


def add_klines_to_db(klines: list[KlinesModel]) -> int:
    result = 0
    if len(klines) == 0:
        return 0

    dir_path = 'klines'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    add_df = convert_to_dataframe(klines)
    unique_pairs = add_df[['symbol', 'timeframe']].drop_duplicates()
    unique_pairs_list = list(unique_pairs.itertuples(index=False, name=None))

    for symbol, tf in unique_pairs_list:
        filename = get_filename(symbol, tf)
        mask = (add_df['symbol'] == symbol) & (add_df['timeframe'] == tf)
        symbol_df = add_df[mask].drop(['symbol', 'timeframe'], axis=1)

        try:
            df = pd.read_csv(filename, dtype=col_types)
            df = pd.concat([df, symbol_df]).drop_duplicates()
        except OSError:
            df = symbol_df

        df.to_csv(filename, index=False)
        result += len(df)

    return result


def read_symbols(timeframe: str | None) -> list[str]:
    """Function read and return list of unique symbols in DB"""
    try:
        file_list = os.listdir('klines')
    except FileNotFoundError:
        return []
    if timeframe:
        symbols = [file_name.split('_')[0] for file_name in file_list if file_name.split(
            '_')[1].startswith(timeframe)]
    else:
        symbols = [file_name.split('_')[0] for file_name in file_list]
    symbols = list(dict.fromkeys(symbols))
    return sorted(symbols)


def read_klines(
        symbol: str,
        timeframe: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int | None = None,
        limit_first: int | None = None,
) -> pd.DataFrame:
    filename = get_filename(symbol, timeframe)
    try:
        df = pd.read_csv(filename, dtype=col_types)
    except FileNotFoundError:
        return pd.DataFrame([])

    if start_date is not None:
        df = df[df['open_time'] > start_date]

    if end_date is not None:
        df = df[df['open_time'] < end_date]

    # Apply limit if it's not None
    if limit is not None:
        df = df.tail(limit)
    elif limit_first is not None:
        df = df.head(limit_first)

    return df
