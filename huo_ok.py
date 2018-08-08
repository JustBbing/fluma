import sys

import ccxt
import numpy as np
import pandas as pd

from utils import load_setting_file, try_three_times


settings = load_setting_file()


class ExHuo:

    def __init__(self):
        self.exh = ccxt.huobipro({
            'apiKey': settings['active']['apiKey'],
            'secret': settings['active']['secret']
        })
        self.trade_symbol = settings['trade_symbol']
        self.num_error = settings['active_exchange_num_error']
        self.money_error = settings['active_exchange_money_error']

    @try_three_times
    def fetch_ex_ohlcv(self):
        # 返回bar数据,最新的数据在0
        kline_1m = self.exh.fetch_ohlcv(symbol=self.trade_symbol, limit=150)
        return kline_1m

    @try_three_times
    def fetch_ex_order(self, order_id):
        return self.exh.fetch_order(order_id)

    @try_three_times
    def fetch_ex_trades(self):
        # 返回值是一个列表,交易历史最新的的放在-1
        return self.exh.fetch_trades(self.trade_symbol, limit=150)

    @try_three_times
    def cancle_ex_order(self, order_id):
        try:
            return self.exh.cancel_order(order_id, symbol=self.trade_symbol)
        except Exception as e:
            if 'the order state is error' in str(e):
                return
            else:
                raise Exception()

    @try_three_times
    def fetch_ex_depth(self):
        # 交易深度,买一卖一为0
        return self.exh.fetch_order_book(self.trade_symbol)

    @try_three_times
    def create_ex_order(self, signal, price, amount):
        try:
            return self.exh.create_order(self.trade_symbol, 'limit', signal, amount, price)
        except Exception as e:
            if self.money_error in str(e):
                print('钱不够买入')
                sys.exit(1)
            elif self.num_error in str(e):
                print('币不够卖出')
                sys.exit(1)
            else:
                raise Exception()

    @try_three_times
    def create_ex_market_order(self, signal, amount, price=None):
        return self.exh.create_order(self.trade_symbol, 'market', signal, amount, price=price)


class ExOke:

    def __init__(self):
        self.exo = ccxt.okex(
            {
                'apiKey': settings['inactive']['apiKey'],
                'secret': settings['inactive']['secret']
            }
        )
        self.trade_symbol = settings['trade_symbol']
        self.num_error = settings['inactive_exchange_num_error']
        self.money_error = settings['inactive_exchange_money_error']

    @try_three_times
    def fetch_ex_ohlcv(self):
        # 返回bar数据,最新的在-1
        kline_1m = self.exo.fetch_ohlcv(symbol=self.trade_symbol, timeframe='1m', since=None, limit=150)
        return kline_1m

    @try_three_times
    def fetch_ex_order(self, order_id):
        return self.exo.fetch_order(order_id, symbol=self.trade_symbol)

    @try_three_times
    def fetch_ex_trades(self):
        # 返回最近的交易记录,最新的在-1
        return self.exo.fetch_trades(self.trade_symbol, limit=150)

    @try_three_times
    def cancle_ex_order(self, order_id):
        try:
            return self.exo.cancel_order(order_id, symbol=self.trade_symbol)
        except Exception as e:
            # TODO　这个错误信息是火币的,okex是否一样
            if 'the order state is error' in str(e):
                return

            else:
                raise Exception()

    @try_three_times
    def fetch_ex_depth(self):
        # 交易深度,卖一买一为0
        return self.exo.fetch_order_book(self.trade_symbol)

    @try_three_times
    def create_ex_order(self, signal, price, amount):
        try:
            return self.exo.create_order(self.trade_symbol, 'limit', signal, amount, price)
        except Exception as e:
            if self.money_error in str(e):
                print('钱不够买入')
                sys.exit(1)
            elif self.num_error in str(e):
                print('币不够卖出')
                sys.exit(1)
            else:
                raise Exception()

    @try_three_times
    def create_ex_market_order(self, signal, amount, price=None):
        return self.exo.create_order(self.trade_symbol, 'market', signal, amount, price=price)


if __name__ == '__main__':
    huo = ExHuo()
    trades = huo.fetch_ex_trades()[-10::]
    for i in trades:
        print(i)
