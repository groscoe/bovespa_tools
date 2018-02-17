'''
read_bovespa.py - helper functions to interpret records from
BOVESPA's historical data.

Raw data available in http://http://www.bmfbovespa.com.br
'''
import pandas as pd
from collections import OrderedDict
from datetime import date
from datetime import datetime as dt
from typing import Any, Callable, Iterator, Union, Dict


RecordTypes = Union[int, str, float, date]
FilePath = str
CharStream = Iterator[str]
BovespaDateString = str


def read_bovespa_file(filename: FilePath) -> pd.DataFrame:
    '''read a file in the BOVESPA historical records format as a
    Pandas' DataFrame.'''
    with open(filename, 'r') as bovespa_file:
        bovespa_file.readline()  # skip file header
        records = (read_bovespa_record(line)
                   for line in skip_last(bovespa_file))

        return pd.DataFrame(records)


def read_bovespa_record(line: str) -> Dict[str, RecordTypes]:
    '''read a line from a BOVESPA style record as a dictionary.'''
    read = read_from(line)
    skip = read  # just for clarity
    record: Dict[str, RecordTypes] = OrderedDict()

    skip(2)  # register type code (always "01")
    record["date"] = read_date(read(8))
    skip(2)  # BDI code
    record["symbol"] = read(12)
    skip(3)  # market type code
    record["company_name"] = read(12)
    record["type"] = read(10)
    skip(3)  # market time in days
    record["currency"] = read(4)
    record["open"] = read_float(read(13))
    record["high"] = read_float(read(13))
    record["low"] = read_float(read(13))
    record["mean"] = read_float(read(13))
    record["close"] = read_float(read(13))
    skip(13)  # best buy offer price
    skip(13)  # best sell offer price
    skip(5)  # number of exchanges for the asset
    record["quantity"] = int(read(18))
    record["volume"] = read_float(read(18))
    record["option_strike_price"] = read_float(read(13))
    skip(1)  # price correction indicator
    record["option_expiry_date"] = read_date(read(8))

    return record


def read_from(string: str) -> Callable[[int], str]:
    '''return a function to read a string as a stream.'''
    it: CharStream = iter(string)
    return lambda n: read_n(n, it)


def read_n(n: int, iterator: CharStream) -> str:
    '''read n chars from a string iterator, join them and strip whitespace.'''
    return ''.join([next(iterator, '') for _ in range(n)]).strip()


def read_date(date_string: BovespaDateString) -> date:
    '''read a string as a date in "YYYYMMDD" format.'''
    date_format = "%Y%m%d"  # from http://strftime.org/
    return dt.strptime(date_string, date_format).date()


def read_float(value: str, after_comma: int = 2) -> float:
    '''read a float from a string of integers with decimal place metadata.'''
    integral_part = value[:-after_comma]
    fractional_part = value[-after_comma:]

    return float(f'{integral_part}.{fractional_part}')


def skip_last(iterator: Iterator[Any]) -> Iterator[Any]:
    '''Yield all but the last value of an iterator.'''
    try:
        next_value = next(iterator)
        for element in iterator:
            yield next_value
            next_value = element
    except StopIteration:
        raise ValueError("Iterator is empty")
