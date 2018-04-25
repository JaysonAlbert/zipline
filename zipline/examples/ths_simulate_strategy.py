# coding=utf-8

from zipline.api import order, record, symbol, cancel_order
import platform
import six


def initialize(context):
    context.smb = symbol('002450')
    print("start init....")
    print(context.portfolio)
    print(context.positions())


sell_status = False
buy_status = False


def handle_data(context, data):
    global sell_status, buy_status
    can_trade = data.can_trade(context.smb)
    current = data.current(symbol('002450'), 'price')
    print(current)
    # hist = data.history(symbol('600496'), bar_count=20, frequency='1m', fields='open')
    # print(hist)
    print(datetime.datetime.now())
    print(context.portfolio)
    print(context.portfolio.positions)

    orders = context.get_open_orders()
    if orders and len(orders) > 0:
        real_order = six.next(six.itervalues(orders))[0]
        cancel_order(real_order)
    if not sell_status:
        order(symbol('002450'), -200, limit_price=current - 0.01)
        sell_status = True
    if not buy_status:
        buy_price = current - 0.1
        if buy_price * 100 <= context.portfolio.cash:
            order(symbol('002450'), 300, limit_price=buy_price)
            buy_status = True


if __name__ == '__main__':
    from zipline.utils.cli import Date
    from zipline.utils.run_algo import run_algorithm
    from zipline.gens.brokers.tdx_shipane_broker import TdxShipaneBroker
    from zipline.gens.shipane_client import ShipaneClient
    import pandas as pd
    import os
    import datetime

    if platform.architecture()[0] == '32bit':
        client_uri = 'config.json'
    else:
        client_uri = "tcp://127.0.0.1:4242"

    shipane_client = ShipaneClient(client_key="")
    broker = TdxShipaneBroker(client_uri, shipane_client)
    os.environ['ZIPLINE_ROOT'] = "G:\\zipline"
    print(os.environ.get("ZIPLINE_ROOT"))
    if not os.path.exists('tmp'):
        os.mkdir('tmp')
    realtime_bar_target = 'tmp/real-bar-{}'.format(str(pd.to_datetime('today').date()))
    state_filename = 'tmp/live-state'

    start = Date(tz='utc', as_timestamp=True).parser('2017-10-01')

    end = Date(tz='utc', as_timestamp=True).parser(datetime.datetime.now().strftime("%Y-%m-%d"))
    run_algorithm(start, end, initialize, 10e6, handle_data=handle_data, bundle='tdx',
                  trading_calendar='SHSZ', data_frequency="minute", output='out.pickle',
                  broker=broker, state_filename=state_filename, realtime_bar_target=realtime_bar_target)