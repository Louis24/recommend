#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  @Time    : 2020/5/21 11:02
#  @Author  : Louis Li
#  @Email   : vortex750@hotmail.com


import json
import pandas as pd
import mysql.connector
from collections import Counter
# from utils import v_distance
from kafka import Message

pd.options.mode.chained_assignment = None
pd.set_option("display.max_columns", None)

"""
更新内容：返回所有热点
"""


class MySQL:
    def __init__(self, database="taxi"):
        self.user = "tmkj"
        self.password = "Tmkj@taxi123"
        self.host = "192.168.245.34"
        self.database = database
        self.cnx = mysql.connector.connect(user=self.user,
                                           password=self.password,
                                           host=self.host,
                                           database=self.database)

    def t_taxi_income_week(self):
        sql = "select * FROM t_taxi_income_week"
        df = pd.read_sql(sql, self.cnx)
        df = df[["vehicle_no", "passenger_on_time", "passenger_on_lng", "passenger_on_lat", "region_id"]]
        df.columns = ["vehicleNo", "time", "lng", "lat", "region_id"]
        df.dropna(axis=0, how='any', inplace=True)
        df.to_csv('t_taxi_income_week.csv', encoding='GBK', index=False)


class Hotspot:
    def __init__(self, scale=0.15, radius=1, lng=113.661042, lat=34.756217):
        self.scale = scale  # 推荐热点的订单总和占总订单比例
        self.radius = radius  # 给出车的经纬度搜索半径radius内的热点
        self.lng = lng  # 推荐该坐标周围的热点
        self.lat = lat  # 推荐该坐标周围的热点

    def analysis(self):
        t_stat_base_region = pd.read_csv("t_stat_base_region.csv", encoding="GBK")

        t_taxi_income_week = pd.read_csv("t_taxi_income_week.csv", encoding="GBK")
        t_taxi_income_week["region_id"] = t_taxi_income_week["region_id"].map(int)
        x = t_taxi_income_week["region_id"].tolist()

        # 按蜂窝数比例推荐
        y = Counter(x)
        n = len(y)
        y = y.most_common(int(n * self.scale))

        cellular = [i[0] for i in y]
        order = [i[1] for i in y]

        # 按照订单数比例推荐
        # y = dict(y)
        #
        # inv = {v: k for k, v in y.items()}
        #
        # z = sorted(y.values(), reverse=True)
        #
        # total = sum(z)
        # top_order = None
        #
        # for i in range(len(z)):
        #
        #     if sum(z[:i + 1]) >= self.scale * total:
        #         top_order = z[:i + 1]
        #         break
        #
        # top_cell = [inv[i] for i in top_order]

        lng = []
        lat = []

        for i in cellular:
            lng_ = t_stat_base_region[t_stat_base_region["id"] == i]["center_lng"].values[0]
            lat_ = t_stat_base_region[t_stat_base_region["id"] == i]["center_lat"].values[0]
            lng.append(lng_)
            lat.append(lat_)

        res = pd.DataFrame()
        res["CELLULAR"] = cellular
        res["ORDER"] = order
        res["LNG"] = lng
        res["LAT"] = lat

        y = []

        for i in res.itertuples():
            x = dict()
            x["cellular"] = i[1]
            x["order"] = i[2]
            x["lng"] = i[3]
            x["lat"] = i[4]
            y.append(x)

        with open('recommend.json', 'w') as f:
            json.dump(y, f)

        res.to_csv('t_taxi_income_week_results.csv', encoding='GBK', index=False, float_format='%.6f')

    @staticmethod
    def kafka():
        hosts = "192.168.245.34:2181,192.168.245.35:2181,192.168.245.36:9092"
        topic = "taxi_recommend"
        message = Message(hosts=hosts, topic=topic)

        with open('recommend.json', 'r') as f:
            x = json.load(f)

            for i in x:
                msg = str(i)
                print(msg)
                message.send(msg)


def main():
    x = MySQL()
    x.t_taxi_income_week()

    y = Hotspot()
    y.analysis()
    # y.kafka()


if __name__ == '__main__':
    main()
