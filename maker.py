"""
第一版:
1.先基于活跃市场最后10次的成交均价为基价,分别乘以1.003和0.997在不活跃市场挂卖单和买单,一方有成交加入买单成交量大于我们的理想
成交量,此时撤掉买单和卖单,不活跃市场开仓成功
2.基于不活跃市场开仓成交价,如100价格买入,分别在不活跃市场和活跃市场挂一个100乘以千分之2的平仓卖单,
3.检查两个平仓卖单的总量是否大于等于自己的买入量,同时监测不活跃市场的最后10次成交均价价是否已经达到止损线,如果两个卖单的总量等于
不活跃市场的开仓量,进入下一个循环,即回到第一步,如果两个市场卖出的量大于不活跃市场开仓量,那么等价于不活跃市场又开仓了,此时
回到第二步,开仓价格为平仓卖出价格,如果是发现要止损,先取消两个平仓订单,再检查两个平仓订单是否在取消的瞬间有完成部分交易,
将开仓数量减去两个平仓订单的完成量作为只损量止损
4.止损时假如是卖出止损,如果当时挂单的深度上买一量比卖一量小且买一卖一价格差不到万分之五,直接以买一价格卖出,
如果买一数量比卖一数量大或者两者的差价大于万分之五,再判断卖一的数量,如果卖一数量非常小,以卖一的价格挂单,如果卖一数量不小,
以卖一的价格减去几个tick的价格卖出
"""

import time
import os
import sys
import logging
import json
import queue
import threading
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import numpy as np
import pandas as pd
import ccxt

from maker.utils import *
from maker.huo_ok import ExHuo, ExOke, settings


class Maker:

    def __init__(self):

        self.start_ex = ExOke()
        self.hedge_ex = ExHuo()

        self.out_percent = settings['out_percent']
        self.profit_percent = settings['profit_percent']
        self.save_path = settings['save_file']
        self.win_percent = settings['win_percent']
        self.price_decimal = settings['price_decimal']
        self.amount_decimal = settings['amount_decimal']
        self.make_amount = settings['move_amount']
        self.min_amount = settings['min_amount']
        self.one_tick = settings['one_tick']

        self.inactive_sell_open_percent = settings['inactive_sell_open_percent']
        self.inactive_buy_open_percent = settings['inactive_buy_open_percent']

        self.loss_q = queue.Queue()
        self.pool = ThreadPoolExecutor(max_workers=3)
        self.check_pool_list = []
        self.data_columns = [
            'create', 'target', 'finish', 'amount', 'filled', 'remaining', 'side', 'status', 'price', 'average'
        ]

    def reinit_ex(self):
        self.inactive_ex = self.start_ex
        self.active_ex = self.hedge_ex

    def switch_ex(self):
        self.inactive_ex = self.hedge_ex
        self.active_ex = self.start_ex

    def check_pool_done(self):
        for i in self.check_pool_list:
            while not i.done():
                time.sleep(0.2)
        self.check_pool_list.clear()
        return True

    def count_times(self):
        reduce_one = timedelta(minutes=60)
        pre_time = datetime.now().strftime("%Y-%m-%d") + ' 23:30:00'
        pre_time = datetime.strptime(pre_time, "%Y-%m-%d %H:%M:%S")
        prt_time = pre_time + reduce_one
        now_date = datetime.today()
        if pre_time < now_date < prt_time:
            time.sleep(3600)
        return True

    def fetch_open_price(self):
        active_lastet_price = self.active_ex.fetch_ex_trades()[-10::]
        price_10_avg = np.average(np.array([one_trade['price'] for one_trade in active_lastet_price]))
        inactive_sell_price = round(price_10_avg * self.inactive_sell_open_percent, self.price_decimal)
        inactive_buy_price = round(price_10_avg * self.inactive_buy_open_percent, self.price_decimal)
        return inactive_buy_price, inactive_sell_price

    def send_maker_orders(self, buy_price, sell_price):
        buy_order = self.pool.submit(self.inactive_ex.create_ex_order, 'buy', buy_price, self.make_amount)
        sell_order = self.pool.submit(self.inactive_ex.create_ex_order, 'sell', sell_price, self.make_amount)
        self.check_pool_list.append(buy_order)
        self.check_pool_list.append(sell_order)
        return buy_order.result()['id'], sell_order.result()['id']

    def check_maker_open_orders(self, buy_id, sell_id):
        while True:
            self.check_pool_done()
            buy_info = self.pool.submit(self.inactive_ex.fetch_ex_order(buy_id))
            sell_info = self.pool.submit(self.inactive_ex.fetch_ex_order(sell_id))
            self.check_pool_list.append(buy_info)
            self.check_pool_list.append(sell_info)
            if buy_info.result()['filled'] > self.min_amount or sell_info.result()['filled'] > self.min_amount:
                self.inactive_ex.cancle_ex_order(sell_id)
                self.inactive_ex.cancle_ex_order(buy_id)
                time.sleep(0.2)
                self.check_pool_done()
                break
            else:
                time.sleep(3)
                continue
        buy_order_info = self.inactive_ex.fetch_ex_order(buy_id)
        sell_order_info = self.inactive_ex.fetch_ex_order(sell_id)
        if buy_order_info['filled'] > sell_order_info['filled']:
            return 'buy', round(buy_order_info['filled']-sell_order_info['filled'], self.amount_decimal)
        else:
            return 'sell', round(sell_order_info['filled']-buy_order_info['filled'], self.amount_decimal)

    def sell_for_circle(self, amount, buy_open_price):
        sell_price = round(buy_open_price*(1+self.win_percent), self.price_decimal)
        inactive_order = self.pool.submit(self.inactive_ex.create_ex_order, 'sell', sell_price, amount)
        active_order = self.pool.submit(self.active_ex.create_ex_order, 'sell', sell_price, amount)
        self.check_pool_list.append(inactive_order)
        self.check_pool_list.append(active_order)
        return inactive_order.result()['id'], active_order.result()['id']

    def buy_for_circle(self, amount, sell_open_price):
        buy_price = round(sell_open_price*(1-self.win_percent), self.price_decimal)
        inactive_order = self.pool.submit(self.inactive_ex.create_ex_order, 'buy', buy_price, amount)
        active_order = self.pool.submit(self.active_ex.create_ex_order, 'buy', buy_price, amount)
        self.check_pool_list.append(inactive_order)
        self.check_pool_list.append(active_order)
        return inactive_order.result()['id'], active_order.result()['id']

    def check_buy_open_is_loss(self, buy_price):
        out_price = buy_price * (1-self.out_percent)
        while True:
            try:
                q_msg = self.loss_q.get(timeout=1)
            except queue.Empty:
                q_msg = None
            if q_msg:
                break
            else:
                inactive_last_10 = self.inactive_ex.fetch_ex_trades()[-10::]
                price_10_avg = np.average(np.array([one_trade['price'] for one_trade in inactive_last_10]))
                if price_10_avg <= out_price:
                    self.loss_q.put('loss')
                    break
                else:
                    continue

    def check_sell_open_is_loss(self, sell_price):
        out_price = sell_price * (1 + self.out_percent)
        while True:
            try:
                q_msg = self.loss_q.get(timeout=1)
            except queue.Empty:
                q_msg = None
            if q_msg:
                break
            else:
                inactive_last_10 = self.inactive_ex.fetch_ex_trades()[-10::]
                price_10_avg = np.average(np.array([one_trade['price'] for one_trade in inactive_last_10]))
                if price_10_avg >= out_price:
                    self.loss_q.put('loss')
                    break
                else:
                    continue

    def check_circle_orders(self, inactive_id, active_id, amount, direction, open_price):
        if direction == 'buy':
            t_spy = threading.Thread(target=self.check_buy_open_is_loss, args=(open_price,))
        else:
            t_spy = threading.Thread(target=self.check_sell_open_is_loss, args=(open_price,))
        t_spy.start()
        while True:
            try:
                q_msg = self.loss_q.get(timeout=1)
            except queue.Empty:
                q_msg = None
            if not q_msg:
                self.check_pool_done()
                inactive_info = self.pool.submit(self.inactive_ex.fetch_ex_order, inactive_id)
                active_info = self.pool.submit(self.active_ex.fetch_ex_order, active_id)
                self.check_pool_list.append(inactive_info)
                self.check_pool_list.append(active_info)
                if inactive_info.result()['filled']+active_info.result()['filled'] >= amount:
                    self.loss_q.put('win')
                    self.inactive_ex.cancle_ex_order(inactive_id)
                    self.active_ex.cancle_ex_order(active_id)
                    time.sleep(0.2)
                    self.check_pool_done()
                    inactive_info = self.inactive_ex.fetch_ex_order(inactive_id)
                    active_info = self.inactive_ex.fetch_ex_order(active_id)
                    return 'win', round(inactive_info['filled'] + active_info['filled'] - amount, self.amount_decimal)
                else:
                    time.sleep(0.5)
                    continue
            else:
                self.inactive_ex.cancle_ex_order(inactive_id)
                self.active_ex.cancle_ex_order(active_id)
                time.sleep(0.2)
                inactive_info = self.inactive_ex.fetch_ex_order(inactive_id)
                active_info = self.inactive_ex.fetch_ex_order(active_id)
                if inactive_info['filled'] + active_info['filled'] < amount:
                    return 'loss', round(amount-inactive_info['filled']-active_info['filled'], self.amount_decimal)
                else:
                    return 'win', round(inactive_info['filled']+active_info['filled']-amount, self.amount_decimal)

    def buy_open_end_round(self, amount, inactive_buy_price):
        inactive_circle_id, active_circle_id = self.sell_for_circle(amount, inactive_buy_price)
        res, amount = self.check_circle_orders(inactive_circle_id, active_circle_id, amount, 'buy', inactive_buy_price)
        if res == 'loss':
            self.cut_loss('sell', amount)
        elif res == 'win' and amount > 0.05:
            return self.sell_open_end_round(amount, inactive_buy_price*(1+self.win_percent))
        elif res == 'win' and amount <= 0.05:
            return
        else:
            print('出现异常状况')
            sys.exit(1)

    def sell_open_end_round(self, amount, inactive_sell_price):
        inactive_circle_id, active_circle_id = self.buy_for_circle(amount, inactive_sell_price)
        res, amount = self.check_circle_orders(inactive_circle_id, active_circle_id, amount, 'sell', inactive_sell_price)
        if res == 'loss':
            self.cut_loss('buy', amount)
            return
        elif res == 'win' and amount > 0.05:
            return self.buy_open_end_round(amount, inactive_sell_price*(1-self.win_percent))
        elif res == 'win' and amount <= 0.05:
            return
        else:
            print('出现异常状况')
            sys.exit(1)

    def cut_loss(self, direction, amount):
        while True:
            if not amount > 0.05:
                return
            depth = self.inactive_ex.fetch_ex_depth()
            if direction == 'buy':
                if depth['bids'][0][1] > depth['asks'][0][1] and depth['asks'][0][0] / depth['bids'][0][0] <= 1.0005:
                    price = depth['asks'][0][0]
                elif depth['bids'][0][1] <= 0.5:
                    price = round(depth['bids'][0][0]+self.one_tick, self.price_decimal)
                else:
                    price = round(depth['bids'][0][0]+self.one_tick*4, self.price_decimal)
                loss_id = self.inactive_ex.create_ex_order('buy', price, amount)
            else:
                if depth['asks'][0][1] > depth['bids'][0][1] and depth['asks'][0][0] / depth['bids'][0][0] <= 1.0005:
                    price = depth['bids'][0][0]
                elif depth['asks'][0][1] <= 0.5:
                    price = round(depth['asks'][0][0]-self.one_tick, self.price_decimal)
                else:
                    price = round(depth['asks'][0][0]-self.one_tick*4, self.price_decimal)
                loss_id = self.inactive_ex.create_ex_order('sell', price, amount)
            time.sleep(1)
            loss_info = self.inactive_ex.fetch_ex_order(loss_id)
            if loss_info['status'] == 'closed':
                return
            else:
                self.inactive_ex.cancle_ex_order(loss_id)
                time.sleep(0.2)
                loss_info = self.inactive_ex.fetch_ex_order(loss_id)
                amount = loss_info['remaining']

    def start_op(self):
        while True:
            inactive_buy_price, inactive_sell_price = self.fetch_open_price()
            buy_id, sell_id = self.send_maker_orders(inactive_buy_price, inactive_sell_price)
            direction, amount = self.check_maker_open_orders(buy_id, sell_id)
            if direction == 'buy':
                self.buy_open_end_round(amount, inactive_buy_price)
            elif direction == 'sell':
                self.sell_open_end_round(amount, inactive_buy_price)



