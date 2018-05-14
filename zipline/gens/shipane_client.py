#!/usr/bin/python
# coding=utf-8
# Author: lhztop

from __future__ import print_function
from enum import Enum
from requests import HTTPError
from shipane_sdk import client
import time
import datetime

import pandas as pd
from zipline.gens.type import Order as TdxOrder
from zipline.gens.type import Position as TdxPosition
from zipline.gens.type import Portfolio as TdxPortfolio
from zipline.gens.type import FIVE_LEVEL_MARKET_ORDER, LIMIT_CHARGE, BUY, SELL
from six import PY2
if not PY2:
    unicode = str  # 兼容python3 rpc 请求 python2的string

if __name__ == '__main__':
    from type import *
else:
    from .type import *

class TradeConstants(object):
    class SoftwareConstants(object):
        SOFTWARE_TDX_ID = 1
        SOFTWARE_TDX_STR = "tdx"   # 通达信
        SOFTWARE_TONGHUASHUN_ID = 2
        SOFTWARE_TONGHUASHUN_STR = "tonghuashun"  # 同花顺

    class OrderConstants(object):
        ORDER_BID = 1
        ORDER_ASK = 2

    class TradeStatusConstants(Enum):
        UN_FINISH = "未成交"
        PARTIAL_FINISH = "部分成交"
        FINISH = "全部成交"
        CANCEL = "撤单"


class Account(object):
    username = ""
    password = ""
    broker = ""
    software = TradeConstants.SoftwareConstants.SOFTWARE_TDX_ID

    def __init__(self, username, password, software=None, broker=None):
        self.username = username
        self.password = password
        self.software = software
        self.broker = broker


class BalanceInfo(object):
    total = 0.0
    frozen = 0.0
    available = 0.0
    market = 0.0

    def __init__(self, total=0.0, frozen=0.0, available=0.0, market=0.0):
        self.total = total
        self.frozen = frozen
        self.available = available
        self.market = market


class Order(object):
    order_id = ""
    order_type = TradeConstants.OrderConstants.ORDER_BID
    stock = ""
    price = 0.0
    volume = 100
    order_at = 0
    status = 0
    tx_at = 0
    create_at = 0
    filled = 0
    average_cost = 0

    def __init__(self, price=0.0, volume=100, stock="", order_type=TradeConstants.OrderConstants.ORDER_BID):
        self.price = price
        self.stock = stock
        self.order_type = order_type
        self.volume = volume
        self.filled = 0
        self.average_cost = 0


class DepositException(RuntimeError):
    """
    资金余额不足
    """
    deserve = 0.0   # 需要的资金
    own = 0.0       # 拥有的资金

    def __init__(self, deserve_amount, own_amount):
        self.deserve = deserve_amount
        self.own = own_amount


class Client(object):

    def __init__(self):
        pass

    @staticmethod
    def parse_status(status_str):
        if u"撤" in status_str:
            return TradeConstants.TradeStatusConstants.CANCEL
        elif u"未成交" in status_str:
            return TradeConstants.TradeStatusConstants.UN_FINISH
        elif u"部分" in status_str:
            return TradeConstants.TradeStatusConstants.PARTIAL_FINISH
        elif u"全部" in status_str:
            return TradeConstants.TradeStatusConstants.FINISH
        else:
            return TradeConstants.TradeStatusConstants.UN_FINISH


class ShipaneClient(Client):
    """
    基本配置：每台机器运行一个实盘易。
    """
    class ShipaneConstants(object):
        class TradeActionConstants(object):
            BID_ACTION = "BUY"
            ASK_ACTION = "SELL"
        class TradeType(object):
            LIMIT_TYPE = "LIMIT"
            MARKET_TYPE = "MARKET"

        ORDER_TYPE_MAP = {TradeActionConstants.BID_ACTION: TradeConstants.OrderConstants.ORDER_BID, TradeActionConstants.ASK_ACTION: TradeConstants.OrderConstants.ORDER_ASK}

    host = "localhost"
    port = 8888
    client_sign = ""
    client_key = ""
    def __init__(self, host="localhost", port=8888, client_sign="", client_key=""):
        self.host = host
        self.port = port
        self.client_sign = client_sign
        self.client_key = client_key
        # self.shipane_client = client.Client(key=self.client_key, client=self.client_sign)


    def _create_client(self):
        shipane_client = client.Client(key=self.client_key, client=self.client_sign)
        return shipane_client

    def portfolio(self):
        """
        目前有BUG。不会计算冻结资金。如果有委买未成交，则会报错500错误。如计算总资产（157808.88）与参考总资产（199741.48）不匹配
        :return:
        """

        spy_client = self._create_client()
        jso = spy_client.get_positions(media_type=client.MediaType.JOIN_QUANT)
        z_portfolio = TdxPortfolio(cash=jso['availableCash'],
                                   positions_value=jso['positionsValue'],
                                   portfolio_value = jso['totalValue'])
        return z_portfolio

    def positions(self):
        spy_client = self._create_client()
        df = spy_client.get_positions()

        z_positions = []
        if df is None or len(df) <= 0:
            return z_positions
        now = datetime.datetime.now()
        for key in df:
            if key == "positions":
                for r in df[key].iterrows():
                    row = r[-1]
                    pos = TdxPosition(sid=row[u"证券代码"],
                                      amount=int(row[u"股票余额"]),
                                      available=int(row[u"可用余额"]),
                                      cost_basis = float(row[u"成本价"]),
                                      last_sale_price = float(row[u"市价"]),
                                      last_sale_date = now)

                    z_positions.append(pos)
        return z_positions

    def order(self, code, volume, price, action, order_type):
        if order_type != LIMIT_CHARGE:
            raise Exception("only support limit order, other order is not supported")
        ret = None
        if action == BUY:
            ret = self.bid(code, price, volume)
        elif action == SELL:
            ret = self.ask(code, price, volume)
        if ret["code"] == 0:
            return ret["order"], None
        return [], None

    def cancel_orders(self, broker_id, order_id):
        return self.cancel(order_id)

    def orders(self):
        ods = self.get_orders()
        rt = {}
        if ods and len(ods) > 0:
            for od in ods:
                rt[od.order_id] = TdxOrder(
                    dt=od.dt,
                    # TODO timezone, zerorpc can't serialize datetime
                    symbol=unicode(od.stock),
                    name=unicode(od.name),
                    status=od.status,
                    price=od.price,
                    amount=od.volume,
                    order_id=od.order_id,
                    average_cost=od.average_cost,
                    filled=od.filled
                )

        return rt

    def bid(self, stock, price, volume):
        """

        :param stock:
        :type stock: str
        :param price:
        :type price: float
        :param volume:
        :type volume: int
        :return:
        """
        amount = price * volume
        # balance = self.balance()
        # if amount > balance.available:
        #     raise DepositException(amount, balance.available)
        spy_client = self._create_client()
        current_time = int(time.mktime(datetime.datetime.now().timetuple()))
        payload = {
          "action": ShipaneClient.ShipaneConstants.TradeActionConstants.BID_ACTION,
          "symbol": stock,
          "type": ShipaneClient.ShipaneConstants.TradeType.LIMIT_TYPE,
          "priceType": 0,
          "price": price,
          "amount": volume
        }
        try:
            jso = spy_client.buy(**payload)
            order = Order()
            order.order_id = jso["id"]
            order.stock = jso["symbol"]
            order.price = jso["price"]
            order.volume = jso["amount"]
            order.order_type = ShipaneClient.ShipaneConstants.ORDER_TYPE_MAP[jso["action"]]
            order.create_at = current_time
            order.order_at = int(time.mktime(datetime.datetime.now().timetuple()))
            return {"code": 0, "order": order}
        except HTTPError as ex:
            status = ex.response.status_code
            if status != 200:
                return {"code": status, "message": ex.response.text}

    def ask(self, stock, price, volume):
        """

        :param stock:
        :type stock: str
        :param price:
        :type price: float
        :param volume:
        :type volume: int
        :return:
        """
        amount = price * volume
        # balance = self.balance()
        # if amount > balance.available:
        #     raise DepositException(amount, balance.available)
        spy_client = self._create_client()
        current_time = int(time.mktime(datetime.datetime.now().timetuple()))
        payload = {
          "action": ShipaneClient.ShipaneConstants.TradeActionConstants.ASK_ACTION,
          "symbol": stock,
          "type": ShipaneClient.ShipaneConstants.TradeType.LIMIT_TYPE,
          "priceType": 0,
          "price": price,
          "amount": volume
        }
        try:
            jso = spy_client.sell(**payload)
            order = Order()
            order.order_id = jso["id"]
            order.stock = jso["symbol"]
            order.price = jso["price"]
            order.volume = jso["amount"]
            order.order_type = ShipaneClient.ShipaneConstants.ORDER_TYPE_MAP[jso["action"]]
            order.create_at = current_time
            order.order_at = int(time.mktime(datetime.datetime.now().timetuple()))
            return {"code": 0, "order": order}
        except HTTPError as ex:
            status = ex.response.status_code
            if status != 200:
                return {"code": status, "message": ex.response.text}

    def map_order_columns(self, pd_colums):
        return {
            "status": u"备注",
            "stock" : u"证券代码",
            "id": u"合同编号",
            "price": u"委托价格",
            "volume": u"委托数量",
            "tx_volume": u"成交数量",
            "average_cost": u"成交均价",
            "side": u"操作"
        }

    def get_orders(self, order_status=None):
        spy_client = self._create_client()
        current_time = int(time.mktime(datetime.datetime.now().timetuple()))
        status = ""
        if order_status:
            if order_status == TradeConstants.TradeStatusConstants.UN_FINISH:
                status = "open"
        df = None
        try:
            df = spy_client.get_orders(status=status)
        except:
            return None
        df_columns = df.columns
        column_map = self.map_order_columns(pd_colums=df_columns)
        lists = []
        for index, row in df.iterrows():
            side = row.ix[column_map["side"]]
            if not (u"买" in side or u"卖" in side):
                continue
            order = Order()
            if u"卖" in side:
                order.order_type = TradeConstants.OrderConstants.ORDER_ASK
            else:
                order.order_type = TradeConstants.OrderConstants.ORDER_BID
            order.dt = unicode(pd.to_datetime("today").date()) + " " + unicode(row[u"委托时间"])
            order.name = unicode(row[u"证券名称"])
            order.order_id = row.ix[column_map["id"]]
            order.stock = row.ix[column_map["stock"]]
            order.price = float(row.ix[column_map["price"]])
            order.volume = int(row.ix[column_map["volume"]])
            order.status = self.parse_status(row.ix[column_map["status"]]).name
            order.average_cost = float(row.ix[column_map["average_cost"]])
            order.filled = float(row.ix[column_map["tx_volume"]])
            if order_status:
                if order.status == order_status:
                    lists.append(order)
            else:
                lists.append(order)

        return lists

    def cancel(self, order_id=0):
        spy_client = self._create_client()
        jso = None
        try:
            if order_id is None or (isinstance(order_id, int) and order_id <= 0):
                jso = spy_client.cancel_all()
            else:
                jso = spy_client.cancel(order_id=order_id)
        except:  # 可能已经成交了，撤单失败
            pass

        return {"code": 0, "data": jso}

    def get_client_info(self, client=None):
        spy_client = self._create_client()
        jso = spy_client.get_account(client=client)
        return jso

    def _transactions(self, start_date, end_date):
        spy_client = self._create_client()
        today = pd.to_datetime('today')
        today_str = today.strftime('%Y%m%d')

        rt = {}

        today_trans = True if today_str == start_date and today_str == end_date else False
        if today_trans:
            df = spy_client.get_orders(status="filled")
        else:
            # 实盘易没有查询历史成交的接口
            return {}

        for index in df.index:
            row = df.ix[index]
            id = row['成交编号']
            sign = -1 if row['操作'] == '卖出' else 1
            commission = float(row["成交金额"]) * 0.0012
            dt = unicode(today.date()) + " " + row["成交时间"]
            rt[id] = Transaction(
                id=id,
                asset=unicode(row["证券代码"]),
                amount=sign * float(row["成交数量"]),
                dt=dt,
                price=float(row["成交均价"]),
                order_id=row["合同编号"],
                # 佣金
                commission=commission
            )
        return rt

    def get_transcations(self):
        start_date = end_date = pd.to_datetime('today').strftime('%Y%m%d')
        return self._transactions(start_date, end_date)

if __name__ == "__main__":
    trade = ShipaneClient(client_key="")
    import jsonpickle
    print(jsonpickle.dumps(trade.get_client_info()))
    print(jsonpickle.dumps(trade.portfolio()))
    print(jsonpickle.dumps(trade.positions()))
    print(jsonpickle.dumps(trade.orders()))
    jso = trade.order("002450", 100, 23, BUY, LIMIT_CHARGE)
    print(jsonpickle.dumps(jso))
    jso = trade.ask("002450", 23, 100)
    jso = trade.get_orders()
    print(jsonpickle.dumps(jso))
    print(jsonpickle.dumps(trade.get_orders(TradeConstants.TradeStatusConstants.UN_FINISH)))
    print(jsonpickle.dumps(trade.cancel()))
    print(jsonpickle.dumps(trade.get_client_info()))
    print(jsonpickle.dumps(trade.get_client_info(client="account:44363925")))
    print(trade.get_transcations())