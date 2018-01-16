# encoding: UTF-8


import os
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP


### return 1 if sh, 0 if sz
def get_stock_type(stock):
    one = stock[0]
    if one == '5' or one == '6' or one == '9':
        return 1

    if stock.startswith("009") or stock.startswith("126") or stock.startswith("110") or stock.startswith(
            "201") or stock.startswith("202") or stock.startswith("203") or stock.startswith("204"):
        return 1

    return 0


def precise_round(num):
    return float(Decimal(str(num)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def fillna(df):
    mask = pd.isnull(df.open)
    df.close.fillna(method='pad', inplace=True)
    df.volume.fillna(0, inplace=True)
    df.loc[mask, ["high", "low", "open"]] = df.loc[mask, "close"]
    return df
