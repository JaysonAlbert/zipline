# encoding: UTF-8

from zipline.gens.brokers.broker import Broker
from zipline.gens.brokers.tdx_broker import TdxBroker, TdxOrder, TdxPortfolio, TdxPosition, TdxTransaction
from six import iteritems
from zipline import protocol
from zipline.finance.execution import (
    MarketOrder,
    LimitOrder,
    StopOrder,
    StopLimitOrder
)
from zipline.finance.order import (
    Order as ZPOrder,
    ORDER_STATUS as ZP_ORDER_STATUS
)
from zipline.gens.type import Transaction as TdxTransaction
from zipline.gens.type import Order as TdxOrder
from zipline.gens.type import Position as TdxPosition
from zipline.gens.type import Portfolio as TdxPortfolio
from zipline.gens.type import OrderRt
from zipline.finance.transaction import Transaction as ZPTransaction
from zipline.api import symbol
from zipline.gens.type import *
import datetime
from logbook import Logger
import pandas as pd
from tdx.engine import Engine
import numpy as np
import zerorpc
import platform
from zipline.errors import SymbolNotFound

if platform.architecture()[0] == '32bit':
    from zipline.gens.tdx_client import TdxClient

log = Logger("TDX_shipane_broker")


class TdxShipaneBroker(TdxBroker):

    def __init__(self, tdx_uri, shipane_client, account_id=None):
        self._shipane_client = shipane_client
        # super(TdxShipaneBroker, self).__init__(tdx_uri, account_id)

    @property
    def positions(self):
        now = datetime.datetime.now()
        z_positions = protocol.Positions()
        for pos in self._shipane_client.positions():
            if isinstance(pos, list):
                pos = TdxPosition(*pos)
            sid = pos.sid
            available = pos.available
            z_position = protocol.Position(symbol(sid))
            z_position.amount = pos.amount
            z_position.cost_basis = pos.cost_basis
            z_position.last_sale_price = pos.last_sale_price
            z_position.last_sale_date = now
            z_positions[symbol(sid)] = z_position

        return z_positions

    @property
    def portfolio(self):
        z_portfolio = protocol.Portfolio()
        pfo = self._shipane_client.portfolio()
        if isinstance(pfo, list):
            pfo = TdxPortfolio(*pfo)
        z_portfolio.capital_used = None  # TODO
        z_portfolio.starting_cash = None
        z_portfolio.portfolio_value = pfo.portfolio_value
        z_portfolio.pnl = None
        z_portfolio.returns = None
        z_portfolio.cash = pfo.cash
        z_portfolio.start_date = None
        z_portfolio.positions = self.positions
        z_portfolio.positions_value = pfo.positions_value
        z_portfolio.position_exposure = z_portfolio.positions_value / (z_portfolio.positions_value + z_portfolio.cash)

        return z_portfolio

    def order(self, asset, amount, style):
        code = asset.symbol

        if amount > 0:
            action = BUY
        else:
            action = SELL

        is_busy = (amount > 0)
        if isinstance(style, MarketOrder):
            order_type = FIVE_LEVEL_MARKET_ORDER
            price = 0.0
        elif isinstance(style, LimitOrder):
            order_type = LIMIT_CHARGE
            price = style.get_limit_price(is_busy)
        elif isinstance(style, StopOrder):
            raise Exception("stop order is not supported")
        elif isinstance(style, StopLimitOrder):
            raise Exception("stop limit order is not supported")

        data, err = self._shipane_client.order(code, abs(amount), price, action, order_type)
        if isinstance(data,list):
            if len(data) > 0:
                data = OrderRt(*data)
            else:
                return None
        order_id = str(data.order_id)
        zp_order = self._get_or_create_zp_order(order_id)

        log.info("Placing order-{order_id}: "
                 "{action} {qty} {symbol} with {order_type}, price = {price}".format(
            order_id=order_id,
            action=action,
            qty=amount,
            symbol=code,
            order_type=order_type,
            price=price
        ))

        return zp_order

    @property
    def orders(self):
        self._update_orders()
        return self._orders

    def _get_or_create_zp_order(self, order_id):
        zp_order_id = self._tdx_to_zp_order_id(order_id)
        if zp_order_id in self.orders:
            return self._orders[zp_order_id]

        if isinstance(order, list):  # handle rpc response for namedtuple object
            order = TdxOrder(*order)
        self._orders[zp_order_id] = self.tdx_order_to_zipline_order(order)
        return self._orders[zp_order_id]

    # amount 可正数可负数
    def _create_zp_order(self, order_id, asset, price, amount, order_type):
        zp_order_id = self._tdx_to_zp_order_id(order_id)
        if zp_order_id in self._orders:
            return self._orders[zp_order_id]

        dt = pd.to_datetime("now", utc=True)
        self._orders[zp_order_id] = ZPOrder(
            dt=dt,
            asset=asset,
            amount=amount,
            stop=None,
            limit=price if order_type is LIMIT_CHARGE else None,
            id=zp_order_id,
            broker_order_id=order_id
        )

        return self._orders[zp_order_id]

    def _tdx_to_zp_order_id(self, order_id):
        return "TDX-{date}-{account_id}-{order_id}".format(
            date=str(pd.to_datetime('today').date()),
            account_id="XXX",
            order_id=order_id
        )

    def tdx_order_to_zipline_order(self, order):
        if order.status is not None and 'CANCEL' == order.status:
            zp_status = ZP_ORDER_STATUS.CANCELLED
        elif order.filled == 0:
            zp_status = ZP_ORDER_STATUS.OPEN
        else:
            zp_status = ZP_ORDER_STATUS.FILLED

        zp_order_id = self._tdx_to_zp_order_id(order.order_id)

        od = ZPOrder(
            dt=pd.to_datetime(order.dt),
            asset=symbol(order.symbol),
            amount=order.amount,
            filled=order.filled,
            stop=None,
            limit=order.price,  # TODO 市价单和限价单
            id=zp_order_id,
        )
        od.broker_order_id = order.order_id
        od.status = zp_status

        return od

    def _update_orders(self):
        ods = self._shipane_client.orders()
        if ods is None or len(ods) <= 0:
            return
        for tdx_order_id, tdx_order in iteritems(ods):
            if isinstance(tdx_order, list):  # handle rpc response for namedtuple object
                tdx_order = TdxOrder(*tdx_order)
            zp_order_id = self._tdx_to_zp_order_id(tdx_order_id)
            self._orders[zp_order_id] = self.tdx_order_to_zipline_order(tdx_order)


    @property
    def transactions(self):
        # TODO: do we need the tx record now?
        t = self._client.transactions()
        rt = {}
        return rt

    def cancel_order(self, order_id):
        if order_id not in self.orders:  # order become transaction, can't cancel
            return
        tdx_order_id = self.orders[order_id].broker_order_id
        self._shipane_client.cancel_orders(0, tdx_order_id)
