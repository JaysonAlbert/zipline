# coding=utf-8

from zipline.api import order, record, symbol, cancel_order
import platform
import six


def initialize(context):
    context.smb = symbol('000651')
    print("start init....")
    print(context.portfolio)
    print(context.positions())


sell_status = False
buy_status = False


def handle_data(context, data):
    global sell_status, buy_status
    can_trade = data.can_trade(context.smb)
    current = data.current(symbol('000651'), 'price')
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
    if not buy_status:
        buy_price = current - 0.1
        print("not buy status.....")
        if buy_price * 100 <= context.portfolio.cash:
            order(symbol('000651'), 100, limit_price=buy_price)
            # buy_status = True
    if not sell_status:
        print("not sell status.....")
        # order(symbol('000651'), -100, limit_price=current - 0.01)
        sell_status = True

def analyze(context=None, results=None):
    import matplotlib.pyplot as plt
    import logbook
    print("start analyze...........")
    logbook.StderrHandler().push_application()
    log = logbook.Logger('Algorithm')

    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    results.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('Portfolio value (格力)')

    ax2 = fig.add_subplot(212)
    ax2.set_ylabel('Price (RMB)')
    if ('000651' in results and 'short_mavg' in results and
            'long_mavg' in results):
        results['000651'].plot(ax=ax2)
        results[['short_mavg', 'long_mavg']].plot(ax=ax2)

        trans = results.ix[[t != [] for t in results.transactions]]
        buys = trans.ix[[t[0]['amount'] > 0 for t in
                         trans.transactions]]
        sells = trans.ix[
            [t[0]['amount'] < 0 for t in trans.transactions]]
        ax2.plot(buys.index, results.short_mavg.ix[buys.index],
                 '^', markersize=10, color='m')
        ax2.plot(sells.index, results.short_mavg.ix[sells.index],
                 'v', markersize=10, color='k')
        plt.legend(loc=0)
    else:
        msg = '000651, short_mavg & long_mavg data not captured using record().'
        ax2.annotate(msg, xy=(0.1, 0.5))
        log.info(msg)

    plt.show()

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
    run_algorithm(start, end, initialize, 10e6, handle_data=handle_data, analyze=analyze, bundle='tdx',
                  trading_calendar='SHSZ', data_frequency="minute", output='out.pickle',
                  broker=broker, state_filename=state_filename, realtime_bar_target=realtime_bar_target)