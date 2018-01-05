# encoding: UTF-8
from tdx.engine import Engine
from zipline.data.bundles.tdx_bundle import *
import os
from zipline.data.bundles import register
from zipline.data import bundles as bundles_module
from functools import partial
from zipline.data.bundles.tdx_bundle import register_tdx
import pandas as pd


def ingest(bundle, assets, minute, start=None, show_progress=True):
    if bundle == 'tdx':
        if assets:
            if not os.path.exists(assets):
                raise FileNotFoundError
            df = pd.read_csv(assets, names=['symbol', 'name'], dtype=str, encoding='utf8')
            register_tdx(df[:1], minute, start)
        else:
            df = pd.DataFrame({
                'symbol': ['000001'],
                'name': ['平安银行']
            })
            register_tdx(df, minute, start)

    bundles_module.ingest(bundle,
                          os.environ,
                          pd.Timestamp.utcnow(),
                          show_progress=show_progress,
                          )


def test_target_ingest():
    # yield ingest, 'tdx', None, True, pd.to_datetime('20170901', utc=True)
    yield ingest, 'tdx', None, False, None


ingest('tdx', None, True, pd.to_datetime('20170101', utc=True), False)
