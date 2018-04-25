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
        self.qydf_list = None

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
            entry_date = entry_date.date()
        elif isinstance(entry_date, str):
            entry_date = pd.Timestamp(entry_date).date()
        elif isinstance(entry_date, pd.tslib.Timestamp):
            entry_date = entry_date.date()

        num, category = self._parse_inerval(interval)

        # 增加report_quarter列
        query = query.add_column(fundamental.quarter)

        # fetch date
        rtn = dict()
        first_record = pd.DataFrame(
            query.filter(fundamental.report_date <= entry_date).group_by(fundamental.code).all()
        )
        if num == 1:
            first_record = self._format_data(first_record, report_quarter)
            return first_record
        elif category == 'd' or category == 'm':
            rtn[str(entry_date)] = self._format_data(first_record, report_quarter)
            for i in range(num - 1):
                delta = '1d'
                if category == 'm':
                    delta = '30d'
                entry_date = entry_date - pd.Timedelta(delta)
                res = pd.DataFrame(
                    query.filter(fundamental.report_date <= entry_date).group_by(fundamental.code).all()
                )
                rtn[str(entry_date)] = self._format_data(res, report_quarter)
            return pd.Panel(rtn).swapaxes(0, 1)
        elif category == 'q':
            entities = self._get_query_entity(query)
            ele = ""
            for i in range(len(entities)):
                ele = ele + entities[i].split('.')[-1] + ","
            ele = ele + "code,report_date,quarter"

            rtn[str(entry_date)] = self._format_data(first_record, report_quarter)
            if self.qydf_list is None or num - 1 > len(self.qydf_list):
                self.get_fundamental_qy(query, entry_date, interval, report_quarter)
                return self._format_qy_data(rtn, num, ele, entry_date, report_quarter)
            else:
                res = pd.DataFrame(
                    query.filter(fundamental.report_date == entry_date).group_by(fundamental.code).all()
                )
                if res.empty:
                    return self._format_qy_data(rtn, num, ele, entry_date, report_quarter)
                else:
                    self._update_qydf_list(res)
                    return self._format_qy_data(rtn, num, ele, entry_date, report_quarter)
        elif category == 'y':
            first_record.index = first_record['code']
            first_record = first_record.drop(['code', 'report_date'], axis=1)
            report_quarter_str = first_record.T.loc['quarter'].iloc[0]
            if not report_quarter:
                first_record = first_record.drop(['quarter'], axis=1)
            rtn[str(entry_date)] = first_record.T
            for i in range(num - 1):
                report_quarter_str, entry_date = self._get_last_report_quarter(category, entry_date, report_quarter_str)
                res = pd.DataFrame(
                    query.filter(fundamental.quarter == report_quarter_str).group_by(fundamental.code).all()
                )
                # 每只股票的report_date不一样，这单纯往前推90天或365天
                rtn[str(entry_date)] = self._format_data(res, report_quarter)
            return pd.Panel(rtn).swapaxes(0, 1)
        else:
            raise Exception('the interval param format wrong, must be 1d, 1m, 1q, 1y, 2y etc...')

    def _format_data(self, df, report_quarter=False):
        df['code'] = ["{:0>6}".format(str(int(i))) for i in df['code'].tolist()]
        df.index = df['code']
        df = df.drop(['code', 'report_date'], axis=1)
        if not report_quarter:
            df = df.drop(['quarter'], axis=1)
        df = df.T
        return df

    def _format_qy_data(self, rtn, num, ele, entry_date, report_quarter):
        for i in range(num - 1):
            entry_date = entry_date - pd.Timedelta("90d")
            df = self.qydf_list[i][ele.split(",")]
            rtn[str(entry_date)] = self._format_data(df, report_quarter)
        return pd.Panel(rtn).swapaxes(0, 1)

    def _parse_inerval(self, interval):
        category = interval[-1]
        num = int(interval[:-1])
        return num, category

    def _get_last_report_quarter(self, category, entry_date, report_quarter):
        if category == 'q':
            report_list = ['年报', '三季报', '中报', '一季报']
            index = 0
            for i in range(len(report_list)):
                if report_quarter.find(report_list[i]) != -1:
                    index = i + 1
                    break
            # 根据report_quarter算出年
            year = int(report_quarter[:4]) - int(index / len(report_list))
            entry_date = entry_date - pd.Timedelta('90d')
            return "%d年%s" % (year, report_list[index]), entry_date
        elif category == 'y':
            year = entry_date.year - 1
            entry_date = entry_date - pd.Timedelta('365d')
            return "%d年%s" % (year, '年报'), entry_date

    def _update_qydf_list(self, df_res):
        df0 = self.qydf_list[0]
        df0_code = df0[df0['code'] == df_res['code']]
        mask = (df0_code['report_date'] != df_res['report_date'])
        df_res = df_res[mask]
        if df_res.empty is not True:
            for i in range(len(self.qydf_list) - 2, -1, -1):
                df = self.qydf_list[i]
                next_df = self.qydf_list[i+1]
                next_df[df['code'] == df_res['code']] = df[df['code'] == df_res['code']]
                self.qydf_list[i] = df
                self.qydf_list[i + 1] = next_df

            df0[df0['code'] == df_res['code']] = df_res
            self.qydf_list[0] = df0


    def get_fundamental_qy(self, query, entry_date=None, interval='1d', report_quarter=False):
        """
        :return: type of list, len @num-1, element is type of pd.Dataframe
        """
        num, category = self._parse_inerval(interval)
        rtn = []
        # 默认取5年的长度
        sql = "select * from fundamental where report_date <= \'%s\' " \
              "and report_date>= \'%s\' order by code, report_date DESC" % (
            entry_date, entry_date - pd.Timedelta('1825d')
        )
        df = pd.read_sql(sql, self.engine)
        group = df.groupby('code')
        subdfs = []
        for i in range(1, num):
            subdfs.append(group.apply(lambda df: self._lamdba_qy(df, i)))
            subdfs[i - 1].dropna(how='all', inplace=True)
        self.qydf_list = subdfs
        return subdfs

    def _lamdba_qy(self, df, num):
        try:
            return df.iloc[num]
        except:
            return

    def _get_query_entity(self, query):
        rtn = []
        const_map = ["max(fundamental.report_date)", "fundamental.code", "fundamental.report_date",
                     "fundamental.quarter"]
        for entity in query._entities:
            if str(entity) not in const_map:
                rtn.append(str(entity))
        return rtn


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
