# encoding=utf-8


from pymongo import MongoClient
import csv
import json
import pandas as pd
import pymysql

import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression

from pylab import *

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False

# Connect to local Database server
# connection = MongoClient('106.55.243.16', 8088)
connection = MongoClient("mongodb://localhost:27017/")
# Connect to local database "Demo", if it does not exist, it will be created
db = connection['demo']
#  Create a collection
t = db['globalSuperStore']

# Connect to mysql
mysqlConn = pymysql.connect(host='localhost', port=3306, user='root', password='123456', database='global_super_store', charset='utf8')
cs = mysqlConn.cursor()

#  Convert CSV data to JSON format
def csvToJson(csvFile):
    jsonData = []
    with open('Global_Superstore2.csv', 'r', encoding='gbk') as f:
        reader = csv.DictReader(f)
        for each in reader:
            jsonData.append(json.dumps(each))
    return jsonData


# Parse json data and write it to mongoDB
def jsonToMongo(jsonData):
    n = 1
    print("Start to write Data：")
    for each in jsonData:
        res = json.loads(each)
        t.insert_one(res)
        # print("Write%d data sucessfully" % n)
        n += 1


# Read data from MongoDB
def dataFromMongo():
    data = pd.DataFrame(list(t.find()))
    return data


# Analyze and count the distribution of categories
def categoryNumsAnalysis(df):
    df["Quantity"] = df["Quantity"].apply(lambda i: int(float(i)))
    categoryNumsDF = df[["Category", "Quantity"]]
    res = categoryNumsDF.groupby("Category").filter(lambda x: x["Quantity"].count() > 1)

    res = res.groupby("Category").agg({"Category": "max", "Quantity": "sum"})

    # res = res[res["Quantity"] <= 10000]
    # res = res[res["Quantity"] != 250]
    # res = res[res["Quantity"] != 234]
    # res = res[res["Quantity"] != 189]
    res = res.sort_values(by=["Quantity"], ascending=False).drop_duplicates("Category")
    # pd.io.sql.to_sql(res, 'td_order_category_quantity', yconnect, schema='global_super_store', if_exists='append')

    # Define insert mysql array
    insertMysqlList = []
    for i in res.values[:20]:
        insertMysqlList.append((i[0], i[1]))
    cs.executemany("insert into td_order_category_quantity values(%s, %s) on duplicate key update category=values(category)", insertMysqlList)
    mysqlConn.commit()
    print("insert data to MySql sucessfully！")
    categoryList = res[["Category"]].values
    x = []
    for i in categoryList[:20]:
        x.append(i[0])

    numsList = res[["Quantity"]].values
    y = []
    for i in numsList[:20]:
        y.append(i[0])

    # Sets the image window size
    plt.figure(figsize=(12, 8), dpi=80)
    plt.bar(range(len(x)), y, width=0.6)
    # The number corresponds to the string one by one, the length of the data is the same, the number of degrees of ratation rotation
    plt.xticks(range(len(x)), x, rotation=90)

    # labelpad    Spacing in points between the label and the x-axis
    plt.xlabel(u"Category", labelpad=10)
    plt.ylabel(u"Quantity", labelpad=10)
    plt.title(u"Category Quantity distribution")
    plt.show()


# Analyze and statistics the distribution of national sales
def regionSalesAnalysis(df):
    df["Sales"] = pd.to_numeric(df["Sales"])
    regionSalesDF = df[["Country", "Sales"]]
    res = regionSalesDF.groupby("Country").agg({"Country": "max", "Sales": "sum"})
    res = res[res["Sales"] != 0].sort_values(by=["Sales"], ascending=False)

    # Define insert mysql array
    insertMysqlList = []
    for i in res.values[:20]:
        insertMysqlList.append((i[0], i[1]))
    cs.executemany("insert into td_order_country_sales values(%s, %s) on duplicate key update country=values(country)", insertMysqlList)
    mysqlConn.commit()
    print("insert data to MySQL sucessfully！")

    categoryList = res[["Country"]].values
    x = []
    for i in categoryList[:20]:
        x.append(i[0])

    salesList = res[["Sales"]].values
    y = []
    for i in salesList[:20]:
        y.append(i[0])

    # Sets the image window size
    plt.figure(figsize=(12, 8), dpi=80)
    plt.bar(range(len(x)), y, width=0.6)
    # The number corresponds to the string one by one, the length of the data is the same, the number of degrees of ratation rotation
    plt.xticks(range(len(x)), x, rotation=90)

    # labelpad    Spacing in points between the label and the x-axis
    plt.xlabel(u"Country", labelpad=10)
    plt.ylabel(u"Sales", labelpad=10)
    plt.title(u"Country sales distribution Top20")
    plt.show()


def modelTrain(x, y, pre_x):
    x = np.array(x).reshape((-1, 1))
    y = np.array(y)
    model = LinearRegression()
    model = model.fit(x, y)
    pre_x = np.array([pre_x]).reshape((-1, 1))

    pre_y = model.predict(pre_x)
    return pre_y


def customerPredict(df):
    regionSalesDF = df[["Customer Name", "Order Date"]]
    res = regionSalesDF[regionSalesDF["Customer Name"] == "Aaron Bergman"].sort_values(by=["Order Date"],
                                                                                       ascending=False)
    dateSet = set()
    for i in res.values:
        name = i[0]
        orderDate = i[1].split("-")[2] + "-" + i[1].split("-")[1] + "-" + i[1].split("-")[0]
        str2Date = datetime.datetime.strptime(orderDate, '%Y-%m-%d')
        dateSet.add(str2Date)

    x = []
    y = []
    initValue = sorted(dateSet)[0]
    cnt = 1
    x.append(cnt), y.append(0)
    for val in sorted(dateSet):
        # Multiple visits by users on the same day are counted only once
        if initValue == val:
            continue
        cnt += 1
        y.append((val - initValue).days)
        x.append(cnt)
        initValue = val

    # Use the model to predict the number of days between next visits
    pre_x = cnt + 1
    pre_y = modelTrain(x, y, pre_x)
    print(pre_y)
    x.append(pre_x)
    y.append(pre_y[0])

    # Insert data into mysql
    insertMysqlList = []
    for idx, val in enumerate(x):
        insertMysqlList.append((x[idx], int(y[idx])))

    cs.executemany("insert into td_order_predict_buy values(%s, %s) on duplicate key update cnt=values(cnt)",
                   insertMysqlList)
    mysqlConn.commit()
    print("insert data to MySQL sucessfully！")

    # Sets the image window size
    plt.figure(figsize=(12, 8), dpi=80)
    plt.bar(range(len(x)), y, width=0.6)
    # The number corresponds to the string one by one, the length of the data is the same, the number of degrees of ratation rotation
    plt.xticks(range(len(x)), x, rotation=90)

    # labelpad    Spacing in points between the label and the x-axis
    plt.xlabel(u"Frequency Order", labelpad=10)
    plt.ylabel(u"Order interval", labelpad=10)
    plt.title(u"Use historical data to predict the next purchase interval")
    plt.show()


def main():
    csvFile = "Global_Superstore2.csv"
    # Convert CSV to JSON format
    jsonData = csvToJson(csvFile)
    # Parse JSON data to write to mongodb
    jsonToMongo(jsonData)
    print("Succeeded in writing data!")

    # Data is read from the mongDB
    df = dataFromMongo()
    print("Reading data successfully!")

    # Analyze and statistic the distribution of classification quantity
    categoryNumsAnalysis(df)
    print("Analyze and statistic the distribution of classification quantity!")

    # Analyze and statistics the distribution of national sales
    regionSalesAnalysis(df)
    print("Analyze and statistics the distribution of national sales!")

    # Customer order predict
    customerPredict(df)
    print("Customer order predict！")

    # Close the mysql
    cs.close()
    mysqlConn.close()


if __name__ == "__main__":
    main()
