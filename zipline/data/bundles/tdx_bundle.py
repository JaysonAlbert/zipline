from tdx.engine import Engine
import pandas as pd
from collections import OrderedDict
import numpy as np
import click
import pytz
import logging as logger
from zipline.utils.util import fillna
from . import core as bundles
from zipline.utils.calendars import get_calendar
from zipline.utils.sqlite_utils import coerce_string_to_eng
from sqlalchemy import create_engine

from zipline.data.us_equity_pricing import (
    SQLITE_ADJUSTMENT_COLUMN_DTYPES,
    SQLITE_SHARES_COLUMN_DTYPES,
    SQLITE_STOCK_DIVIDEND_PAYOUT_COLUMN_DTYPES,
)

from os.path import join
import os
import json
import datetime

from functools import partial
from numpy import searchsorted
from ..schema import (
    Base,
    SessionBar,
    SESSION_BAR_TABLE,
)

logger.basicConfig(level=logger.INFO)

OHLC_RATIO = 1000
DAY_BARS_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "day",
    "id"
]

SCALED_COLUMNS = [
    "open",
    "high",
    "low",
    "close"
]

DATE_DIR = 'dates.json'
SESSION_BAR_DB = 'session-bars.sqlite'


def fetch_symbols(engine, assets=None):
    if assets is not None:
        stock_list = engine.security_list
        stock_list.name = stock_list.name.str.rstrip("\x00")
        stock_list = stock_list[stock_list.code.isin(assets.symbol)]
        stock_list = stock_list[stock_list.name.isin(assets.name)]
    else:
        stock_list = engine.stock_list
    symbols = pd.DataFrame()
    symbols['symbol'] = stock_list.code
    symbols['asset_name'] = stock_list.name
    symbols['exchange'] = 'SHSZ'  # calendar name
    return symbols.reset_index()


def fetch_single_equity(engine, symbol, start=None, end=None, freq='1d'):
    df = engine.get_security_bars(symbol, freq, start, end)
    if df is None or df.empty:
        return df
    df['volume'] = df['vol'].astype(np.int32) * 100  # hands * 100 == shares

    if freq == '1d':
        df['id'] = int(symbol)

    return df.drop(['vol', 'amount', 'code'], axis=1)


def fetch_single_split_and_dividend(engine, symbol):
    df = engine.xdxr(symbol)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    df_sd = df[(df.category == 1) & (df.peigu == 0)]
    if df_sd.empty:
        splits = pd.DataFrame()
        dividends = pd.DataFrame()
    else:
        splits = pd.DataFrame({
            'sid': int(symbol),
            'effective_date': df_sd.index,
            'ratio': 10 / (10 + df_sd.songzhuangu)
        })

        dividends = pd.DataFrame({
            'sid': int(symbol),
            'ex_date': df_sd.index,
            'amount': df_sd.fenhong / 10,
            'record_date': pd.NaT,
            'declared_date': pd.NaT,
            'pay_date': pd.NaT
        })

    dfs = df[df.category != 1]

    if dfs.empty:
        shares = pd.DataFrame()
    else:
        shares = pd.DataFrame({
            'sid': int(symbol),
            'effective_date': dfs.index,
            'shares': dfs.houzongguben,
            'circulation': dfs.panhouliutong,
        })

    return splits, dividends, shares


def fetch_splits_and_dividends(engine, symbols, start=None, end=None):
    all_splits = []
    all_dividends = []
    all_shares = []
    for symbol in symbols['symbol']:
        split, dividend, share = fetch_single_split_and_dividend(engine, symbol)
        all_splits.append(split)
        all_dividends.append(dividend)
        all_shares.append(share)

    if len(all_splits) != 0:
        splits = pd.concat(all_splits)
        dividends = pd.concat(all_dividends)
        shares = pd.concat(all_shares)
    else:
        splits = pd.DataFrame(
            np.array([], dtype=list(SQLITE_ADJUSTMENT_COLUMN_DTYPES.items())),
        )
        dividends = pd.DataFrame(
            np.array([], dtype=list(SQLITE_STOCK_DIVIDEND_PAYOUT_COLUMN_DTYPES.items())),
        )
        shares = pd.DataFrame(
            np.array([], dtype=list(SQLITE_SHARES_COLUMN_DTYPES.items())),
        )
        return splits, dividends, shares

    if start:
        dividends = dividends[dividends.ex_date >= start]
        splits = splits[splits.effective_date >= start]
        shares = shares[shares.effective_date >= start]

    if end:
        dividends = dividends[dividends.ex_date <= end]
        splits = splits[splits.effective_date <= end]
        shares = shares[shares.effective_date <= end]
    return splits[splits.ratio != 1], dividends[dividends.amount != 0], shares


def get_meta_from_bars(df):
    index = df.index
    return OrderedDict([
        ("start_date", index[0]),
        ("end_date", index[-1]),
        ("first_traded", index[0]),
        ("auto_close_date", index[-1])
    ])


def reindex_to_calendar(calendar, data, freq='1d', start_session=None, end_session=None):
    if data.empty:
        return None
    if start_session is None:
        start_session, end_session = data.index[[0, -1]]
    if not isinstance(start_session, pd.Timestamp):
        start_session = pd.Timestamp(start_session, unit='m')
        end_session = pd.Timestamp(end_session, unit='m')

    start_session = start_session.normalize()
    end_session = end_session.normalize()

    if freq == '1d':
        all_sessions = calendar.sessions_in_range(start_session, end_session).tz_localize(None)
        df = data.reindex(all_sessions, copy=False)
        df = fillna(df)
        df.id.fillna(method='pad', inplace=True)
        df.id.fillna(method="bfill", inplace=True)
        df.day = df.index.values.astype('datetime64[m]').astype(np.int64)
    else:
        all_sessions = calendar.minutes_for_sessions_in_range(start_session, end_session).tz_localize(None)
        data.index = data.index.tz_localize(pytz.timezone('Asia/Shanghai')).tz_convert('UTC').tz_localize(None)
        df = data.reindex(all_sessions, copy=False)
        df = fillna(df)

    return df


def tdx_bundle(assets,
               ingest_minute,  # whether to ingest minute data, default False
               fundamental,  # whether to ingest fundamental data, default False
               environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               fundamental_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir):
    eg = Engine(auto_retry=True, multithread=True, best_ip=True, thread_num=1)
    eg.connect()

    symbols = fetch_symbols(eg, assets)
    metas = []

    today = pd.to_datetime('today', utc=True)
    distance = calendar.session_distance(start_session, today)

    dates_path = join(output_dir, DATE_DIR)
    if os.path.isfile(dates_path):
        with open(dates_path, 'r') as f:
            dates_json = json.load(f)
    else:
        dates_json = {
            '1d': {},
            '1m': {}
        }

    session_bars = create_engine('sqlite:///' + join(output_dir, SESSION_BAR_DB))

    def gen_symbols_data(symbol_map, freq='1d'):
        if not session_bars.has_table(SESSION_BAR_TABLE):
            Base.metadata.create_all(session_bars.connect(), checkfirst=True,
                                     tables=[Base.metadata.tables[SESSION_BAR_TABLE]])

        func = partial(fetch_single_equity, eg)
        now = pd.to_datetime('now', utc=True)
        if end_session >= now.normalize():
            end = now.normalize()
            if now.tz_convert('Asia/Shanghai').time() < datetime.time(15, 5):
                end = end - pd.Timedelta('1 D')
        else:
            end = end_session

        if freq == '1m':
            if distance >= 100:
                func = eg.get_k_data

        for index, symbol in symbol_map.iteritems():
            try:
                start = pd.to_datetime(dates_json[freq][symbol], utc=True) + pd.Timedelta('1 D')
                if start > end:
                    if freq == '1d'and symbol in dates_json[freq]:
                        data = pd.read_sql(
                            "select * from {} where id = {} order by day ASC ".format(SESSION_BAR_TABLE, int(symbol)),
                            session_bars, index_col='day')
                        data.index = pd.to_datetime(data.index)
                        yield int(symbol), data
                    else:
                        continue
            except KeyError:
                start = start_session
            data = reindex_to_calendar(
                calendar,
                func(symbol, start, end, freq),
                start_session=start, end_session=end,
                freq=freq,
            )
            if data is None or data.empty:
                if freq == '1d'and symbol in dates_json[freq]:
                    data = pd.read_sql(
                        "select * from {} where id = {} order by day ASC ".format(SESSION_BAR_TABLE, int(symbol)),
                        session_bars, index_col='day')
                    data.index = pd.to_datetime(data.index)
                    yield int(symbol), data
                continue
            if freq == '1d':
                if data.close.isnull()[0]:  # padding fill error if the first is NaN
                    data2 = pd.read_sql(
                        "select * from {} where id = {} order by day desc limit 1 ".format(SESSION_BAR_TABLE, int(symbol)),
                        session_bars, index_col='day')
                    data["close"][0] = data2["close"][0]
                    fillna(data)
                data.to_sql(SESSION_BAR_TABLE, session_bars.connect(), if_exists='append', index_label='day')
                if symbol in dates_json[freq]:
                    data = pd.read_sql(
                        "select * from {} where id = {} order by day ASC ".format(SESSION_BAR_TABLE, int(symbol)),
                        session_bars, index_col='day')
                    data.index = pd.to_datetime(data.index)
            dates_json[freq][symbol] = data.index[-1].strftime('%Y%m%d')
            yield int(symbol), data

            with open(dates_path, 'w') as f:
                json.dump(dates_json, f)

    symbol_map = symbols.symbol

    assets = set([int(s) for s in symbol_map])
    daily_bar_writer.write(gen_symbols_data(symbol_map, freq="1d"), assets=assets, show_progress=show_progress)

    splits, dividends, shares = fetch_splits_and_dividends(eg, symbols, start_session, end_session)
    metas = pd.read_sql("select id as symbol,min(day) as start_date,max(day) as end_date from bars group by id;",
                        session_bars,
                        parse_dates=['start_date','end_date']
                        )
    metas['symbol'] = metas['symbol'].apply(lambda x:format(x,'06'))
    metas['first_traded'] = metas['start_date']
    metas['auto_close_date'] = metas['end_date']

    symbols = symbols.set_index('symbol', drop=False).join(metas.set_index('symbol'), how='inner')
    asset_db_writer.write(symbols)
    adjustment_writer.write(
        splits=splits,
        dividends=dividends,
        shares=shares
    )

    if fundamental:
        logger.info("writing fundamental data:")
        try:
            fundamental_writer.write(start_session, end_session)
        except Exception as e:
            pass

    if ingest_minute:
        with click.progressbar(gen_symbols_data(symbol_map, freq="1m"),
                               label="Merging minute equity files:",
                               length=len(assets),
                               item_show_func=lambda e: e if e is None else str(e[0]),
                               ) as bar:
            minute_bar_writer.write(bar, show_progress=False)

    eg.exit()


def register_tdx(assets=None, minute=False, start=None, fundamental=False, end=None):
    try:
        bundles.unregister('tdx')
    except bundles.UnknownBundle:
        pass
    calendar = get_calendar('SHSZ')
    if start:
        if not calendar.is_session(start):
            start = calendar.all_sessions[searchsorted(calendar.all_sessions, start)]
    bundles.register('tdx', partial(tdx_bundle, assets, minute, fundamental), 'SHSZ', start, end,
                     minutes_per_day=240)


bundles.register('tdx', partial(tdx_bundle, None, False, False), minutes_per_day=240)

if __name__ == '__main__':
    eg = Engine(auto_retry=True, multithread=True, thread_num=8)
    with eg.connect():
        symbols = fetch_symbols(eg)
        symbols = symbols[:3]
        data = []
        metas = []
        for symbol in symbols.symbol:
            data.append((int(symbol), fetch_single_equity(eg, symbol)))
            metas.append(get_meta_from_bars(data[-1][1]))
        symbols = pd.concat([symbols, pd.DataFrame(data=metas)], axis=1)
        splits, dividends = fetch_splits_and_dividends(eg, symbols)
