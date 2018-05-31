#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import httplib, urllib
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

# 1. 连接mysql数据库
# 2. http请求高德地图，获取省、市、区和街道数据，两种请求条件
# *****2.1 输入“中国”，返回省份数据
# *****2.2 输入“省份”，返回市、区数据
# *****2.3 输入“区”，  返回街道数据
# 3. json数据转换为结构化数据
# 4. 结构化数据存储在地址库中


uri = "/v3/config/district"
key = "05a389c77f3a73e812f7d8156d56175b"
headers = { "Context-type": "application/json" }
gaode_conn = httplib.HTTPConnection("restapi.amap.com")

# 获取经纬度
def getLocation(location):
    data = location.split(",")
    try:
        a = float(data[0])
        b = float(data[1])
    except:
        a = 0.0
        b = 0.0
    return a, b

def ConnectDB():
    db = MySQLdb.connect(host="127.0.0.1", port=3306, user="user", passwd="password", db="addresses", charset="utf8")
    cursor = db.cursor()
    return cursor, db

def InsertProvinces(jsonresp, db):
    provincesInfos = jsonresp["districts"][0]["districts"] ## ["根节点"]["中国"]
    for provinceInfo in provincesInfos:
        citycode = provinceInfo["citycode"]  
        longtitude, latitude = getLocation(provinceInfo["center"])
        adcode = provinceInfo["adcode"]
        name = provinceInfo["name"]
        #cursor.execute("""INSERT INTO provinces(name, citycode, adcode, center_longtitude, center_latitude, weight_value) VALUES (%s, %s, %s, %s, %s, 0)""", [name, citycode, adcode, longtitude, latitude])
        query = "INSERT INTO provinces(name, citycode, adcode, center_longtitude, center_latitude, weight_value) VALUES(%s, %s, %s, %s, %s, 0)"
        ## 没有注意的坑
        if type(citycode) == list:
            citycode = ''
        args = (name, citycode, adcode, longtitude, latitude)
        cursor.execute(query, args)
    db.commit()
    return

def InsertCities(jsonresp, db):
    provincesInfos = jsonresp["districts"][0]["districts"] ## ["根节点"]["中国"]
    for index in range(len(provincesInfos)):
        # 1. province_id
        cursor.execute("SELECT province_id  FROM provinces WHERE name='%s'"% (provincesInfos[index]["name"]))
        row = cursor.fetchone()
        if row == None:
            print provincesInfos[index]["name"]
            return
        province_id = row[0]
        for cityInfo in provincesInfos[index]["districts"]:
            name = cityInfo["name"]
            citycode = cityInfo["citycode"]
            adcode = cityInfo["adcode"]
            longtitude, latitude = getLocation(cityInfo["center"])
            print name.encode('utf-8')
            query = "INSERT INTO cities(name, citycode, adcode, center_longtitude, center_latitude, weight_value, province_id) VALUES(%s, %s, %s, %s, %s, 0, %s)"
            if type(citycode)==list:
                citycode = ""
            args = (name, citycode, adcode, longtitude, latitude, province_id)
            cursor.execute(query, args)
        db.commit()
    return

def InsertDistricts(jsonresp, db):
    provincesInfos = jsonresp["districts"][0]["districts"] ## ["根节点"]["中国"]
    for index in range(len(provincesInfos)):
        citiesInfos = provincesInfos[index]["districts"]
        for sub_index in range(len(citiesInfos)): ## 城市
            ## 1. province_id, city_id
            cursor.execute("SELECT province_id, city_id FROM cities WHERE name='%s'"%(citiesInfos[sub_index]["name"]))
            row =  cursor.fetchone()
            if row == None:
                print "can't find city:", citiesInfos[sub_index]["name"]
                return
            province_id = row[0]
            city_id = row[1]
            districtsInfos = citiesInfos[sub_index]["districts"]
            for three_index in range(len(districtsInfos)):
                citycode = districtsInfos[three_index]["citycode"]
                adcode = districtsInfos[three_index]["adcode"]
                name = districtsInfos[three_index]["name"]
                longtitude, latitude = getLocation(districtsInfos[three_index]["center"])
                query = "INSERT INTO districts(name, adcode, citycode, province_id, city_id, center_longtitude, center_latitude, weight_value) VALUES(%s, %s, %s, %s, %s, %s, %s, 0)"
                if type(citycode) == list:
                    citycode=""
                args = (name, adcode, citycode, province_id, city_id, longtitude, latitude)
                cursor.execute(query, args)
            db.commit()
    return

def HttpRequest(params):
    gaode_conn.request("GET", "/v3/config/district?"+params, '', headers)
    response = gaode_conn.getresponse()
    data = response.read().decode('utf-8')
    ## json数据转换为python对象
    jsonresp = json.loads(data)
    return jsonresp

def InsertPCD(db):
    ## 2. http请求高德地图， 输入2.1条件: [ "中国", 3 ] , 返回：省-> 市-> 区/县
    params = urllib.urlencode({'key': key, 'keywords': '中国', 'subdistrict': 3, 'showbiz': 'false', 'extensions': 'base'})
    jsonresp = HttpRequest(params)
    #  说明：于以下3，4，5的操作, 独立的
    ## 3. 插入省份表
    InsertProvinces(jsonresp, db)
    ## 4. 插入城市表
    InsertCities(jsonresp, db)
    ## 5. 插入区/县表
    InsertDistricts(jsonresp, db)
    return

def InsertStreets(offset, limit, db):
    ## 获取所有区的名称和区域编码
    cursor.execute("SELECT name, adcode, province_id, city_id, district_id FROM districts ORDER BY name DESC LIMIT %s, %s"% (offset*limit, limit))
    rows = cursor.fetchall()
    if rows == None:
        return
    ## key=05a389c77f3a73e812f7d8156d56175b&keywords=南山区&subdistrict=3&showbiz=false&extensions=base&filter=440305
    for row in rows:
        params = urllib.urlencode({'key': key, 'keywords': str(row[0]), 'subdistrcit': 3, 'showbiz': 'false', 'extensions': 'base', 'filter': str(row[1])})
        jsonresp = HttpRequest(params)
        streets = jsonresp["districts"][0]["districts"]
        for street in streets:
            citycode = street["citycode"]
            adcode = street["adcode"]
            name = street["name"]
            longtitude, latitude = getLocation(street["center"])
            query = "INSERT INTO streets(name, adcode, citycode, province_id, city_id, district_id, center_longtitude, center_latitude, weight_value) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, 0)"
            args = (name, adcode, citycode, row[2], row[3], row[4], longtitude, latitude)
            cursor.execute(query, args)
        db.commit()
    return

if __name__ == '__main__':
    ## 1.连接mysql数据库
    cursor, db = ConnectDB()
    ## 2. 插入省、市，区表数据
    InsertPCD(db)
    ## 3. 新增街道数据
    offset = int(input("从第几页开始？offset="))
    limit = int(input("每页多少条数据？, 默认每页1000条。limit="))
    if limit == 0 or offset < 0 :
        offset = 0
        limit = 1000
    InsertStreets(offset, limit, db)

    db.close()
