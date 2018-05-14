# encoding: UTF-8
from tdx.engine import Engine
from zipline.data.bundles.tdx_bundle import *
import os
from zipline.data.bundles import register
from zipline.data import bundles as bundles_module
from functools import partial
from zipline.data.bundles.tdx_bundle import register_tdx
import pandas as pd
import logbook
import os


def ingest(bundle, assets, minute, start=None, show_progress=True,
           localdb_ip="",
           localdb_port=27017
           ):
    if bundle == 'tdx':
        if assets:
            if not os.path.exists(assets):
                raise FileNotFoundError
            df = pd.read_csv(assets, names=['symbol', 'name'], dtype=str, encoding='utf8')
            register_tdx(df[:10], minute, start)
        else:
            df = pd.DataFrame({
                'symbol': ['000001'],
                'name': ['平安银行']
            })
            df = None
            register_tdx(df, minute, start, localdb_ip=localdb_ip, localdb_port=localdb_port)

    bundles_module.ingest(bundle,
                          os.environ,
                          pd.Timestamp.utcnow(),
                          show_progress=show_progress,
                          )


def test_target_ingest():
    # yield ingest, 'tdx', None, True, pd.to_datetime('20170901', utc=True)
    yield ingest, 'tdx', None, False, None


# ingest('tdx', "ETF.csv", True, pd.to_datetime('20170101', utc=True), True)
logbook.StderrHandler().push_application()
os.environ['ZIPLINE_ROOT'] = "G:\\zipline"
print(os.environ.get("ZIPLINE_ROOT"))
ingest('tdx', "", True, show_progress=True,
       localdb_ip="192.168.0.114", localdb_port=27016)

# if __name__ == '__main__':
#     from tdx.engine import AsyncEngine
#     from functools import partial
#
#     # '115.238.90.165', 60.191.117.167, 218.75.126.9, 124.160.88.183, '60.12.136.250'
#     # '218.108.98.244','218.108.47.69'
#     listx = ['180.153.18.170', '180.153.18.171', '202.108.253.130', '202.108.253.131',
#              '180.153.39.51']
#     for ip in listx:
#         print('test ip: %s' % (ip))
#         aeg = AsyncEngine(ip=ip, raise_exception=True)
#         aeg.connect()
#         start_session = pd.Timestamp('1990-12-19 00:00:00+00:00')
#         end_session = pd.Timestamp('2019-01-25 00:00:00+00:00')
#         func = partial(async_fetch_single_euqity, aeg)
#         try:
#             euqitiesList = func(['600094', '600664'], start_session, end_session, '1d')
#         except ValueError:
#             print('worse ip: %s' % (ip))


