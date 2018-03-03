import tushare as ts
from logbook import Logger
import click
from functools import wraps
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.sql import (
    func
)
from sqlalchemy.orm import (
    sessionmaker,
    Query,
)
from toolz import first
from zipline.data.schema import (
    full,
    fundamental,
    Base
)
import pandas as pd
import click
from zipline.utils.preprocess import preprocess
from zipline.utils.sqlite_utils import coerce_string_to_eng
import pandas as pd

logger = Logger("fundamental")


def retry(times=3):
    def wrapper(func):
        @wraps(func)
        def fun(*args, **kwargs):
            count = 0
            while count < times:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    count = count + 1

            logger.error("connection failed after retried 3 times. {} {}".format(args,kwargs))
            raise Exception("connection failed after retried 3 times. {} {}".format(args,kwargs))

        return fun

    return wrapper


@retry()
def call_func(name, i, j):
    return eval("ts.get_{}_data({}, {}).drop_duplicates('code').set_index(['code','name'])".format(name, i, j))


def query(*args):
    engine = create_engine("sqlite:///fundamental.sqlite")
    Session = sessionmaker(bind=engine)
    session = Session()
    args = [fundamental.code, fundamental.report_date, fundamental.roe,
            func.max(fundamental.report_date).label('report_date')]
    return Query(args).with_session(session).filter(
        fundamental.report_date < pd.to_datetime('2013-07-18')
    ).group_by(fundamental.code)


def query1(*args):
    engine = create_engine("sqlite:///fundamental.sqlite")
    Session = sessionmaker(bind=engine)
    session = Session()
    args = [full.code, full.report_date, full.roe]
    return Query(args).with_session(session).filter(
        full.trade_date == pd.to_datetime('2013-07-18')
    )


def sql_query():
    data = query(fundamental.code, fundamental.report_date, fundamental.roe,
                 fundamental.quarter).filter(
        fundamental.roe > 10
    ).all()

    df = pd.DataFrame(data)

    print(df)

    data = query1().filter(
        full.roe > 10
    ).all()

    df = pd.DataFrame(data)

    print(df)


class FundamentalReader(object):

    @preprocess(engine=coerce_string_to_eng)
    def __init__(self, engine):
        self.engine = engine
        self.session = sessionmaker(bind=self.engine)()

    def query(self, dt, *args, **kwargs):
        args = list(args) + [fundamental.code, func.max(fundamental.report_date).label('report_date')]
        # return Query(args).with_session(self.session).filter(
        #     fundamental.report_date < dt
        # )
        return Query(args).with_session(self.session)

    def get_fundamental(self, query, entry_date=None, interval='1d', report_quarter=False):
        # params check
        if not entry_date:
            entry_date = pd.Timestamp('now').normalize() - pd.Timedelta('1d')
        if isinstance(entry_date, str):
            entry_date = pd.Timestamp(entry_date)

        num, type = self._parse_inerval(interval)

        if report_quarter:
            query = query.add_column(fundamental.report_quarter)

        # fetch date
        rtn = dict()
        first_record = pd.DataFrame(
            query.filter(fundamental.report_date <= entry_date).group_by(fundamental.code).all()
        )
        first_record.index = first_record['code']
        first_record = first_record.drop(['code', 'report_date'], axis=1).T
        if num == 1:
            return first_record
        elif type == 'd' or type == 'm':
            rtn[str(entry_date.date())] = first_record
            for i in range(num - 1):
                delta = '1d'
                if type == 'm':
                    delta = '30d'
                entry_date = entry_date - pd.Timedelta(delta)
                res = pd.DataFrame(
                    query.filter(fundamental.report_date <= entry_date).group_by(fundamental.code).all()
                )
                res.index = res['code']
                res = res.drop(['code', 'report_date'], axis=1).T
                # 返回空数据
                # if len(res):
                rtn[str(entry_date.date())] = res
            return pd.Panel(rtn).swapaxes(0, 1)
        elif type == 'q' or type == 'y':
            rtn[str(entry_date.date())] = first_record
            report_quarter = first_record.loc['report_quarter'].iloc[0]
            for i in range(num - 1):
                report_quarter, entry_date = self._get_last_report_quarter(type, entry_date, report_quarter)
                res = pd.DataFrame(
                    query.filter(fundamental.report_quarter == report_quarter).group_by(fundamental.code).all()
                )
                res.index = res['code']
                res = res.drop(['code'], axis=1).T
                # 每只股票的report_date不一样，这单纯往前推1个季度或一年
                rtn[str(entry_date.date())] = res
            return pd.Panel(rtn).swapaxes(0, 1)
        else:
            raise Exception('the interval param format wrong, must be 1d, 1m, 1q, 1y, 2y etc...')



        return pd.DataFrame(query.group_by(fundamental.code).all())

    def _parse_inerval(self, interval):
        type = interval[-1]
        num = int(interval[:-1])
        return num, type

    def _get_last_report_quarter(self, type, entry_date, report_quarter):
        if type == 'q':
            report_list = ['年报', '三季报', '中报', '一季报']
            index = 0
            for i in range(len(report_list)):
                if report_quarter.find(report_list[i]) != -1:
                    index = i + 1
                    break
            year = entry_date.year - index % len(report_list)
            entry_date = entry_date - pd.Timedelta('90d')
            return "%d年%s" % (year, report_list[index]), entry_date
        elif type == 'y':
            year = entry_date.year - 1
            entry_date = entry_date - pd.Timedelta('365d')
            return "%d年%s" % (year, '年报'), entry_date




class FundamentalWriter(object):
    table_names = ['fundamental', 'full']

    @preprocess(engine=coerce_string_to_eng)
    def __init__(self, engine):
        self.engine = engine

    def write(self, start, end):
        self.init_db(self.engine)

        start = max(2010, int(start.strftime('%Y')))
        end = int(min(pd.to_datetime('today',utc=True),end).strftime('%Y')) + 1

        pp = [(i, j) for i in range(start, end) for j in range(1, 5)]

        for i in pp:
            self.quarter_report(*i)
            print(i)

    def fill(self):
        self.init_db(self.engine)
        df = pd.read_sql("select * from fundamental", self.engine).sort_values(['report_date', 'quarter'])
        df['trade_date'] = df['report_date'] = pd.to_datetime(df['report_date'])

        with click.progressbar(df.groupby('code'),
                               label='writing data',
                               item_show_func=lambda x: x[0] if x else None) as bar:
            bar.is_hidden = False
            for stock, group in bar:
                group = group.drop_duplicates(subset='trade_date', keep="last").set_index('trade_date')
                sessions = pd.date_range(group.index[0], group.index[-1])
                d = group.reindex(sessions, copy=False).fillna(method='pad')
                d.to_sql('full', self.engine, if_exists='append', index_label='trade_date')

    def all_tables_presents(self, txn):
        conn = txn.connect()
        for table_name in self.table_names:
            if not txn.dialect.has_table(conn, table_name):
                return False

        return True

    def init_db(self, txn):
        if not self.all_tables_presents(txn):
            Base.metadata.create_all(txn.connect(), checkfirst=True)

    def quarter_report(self, year, quarter):
        func_names = ["report", "profit", "operation", "growth", "debtpaying", "cashflow"]
        dfs = [call_func(name, year, quarter) for name in func_names]

        df = pd.concat(dfs, axis=1).dropna(axis=0, subset=['report_date'])  # drop if no report_date
        df['report_date'] = pd.to_datetime(
            str(year) + '-' + df['report_date'].apply(lambda x: x if x != '02-29' else '02-28'))
        df['quarter'] = quarter
        df.to_sql('fundamental', self.engine, if_exists='append')


if __name__ == '__main__':
    # writer = FundamentalWriter(engine)
    # writer.fill()
    sql_query()

    # engine = create_engine("sqlite:///fundamental.sqlite")
    # fr = FundamentalReader(engine)
    # q = fr.query(20150101)
    # fr.get_fundamental(q)