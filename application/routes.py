from application import app

from flask import Flask, jsonify,render_template, flash, request, redirect, url_for
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('jsfile').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
jsfile = 'ab.json'
df = spark.read.json(jsfile).cache()

def explode(nested):
    list=[]
    for i in range(9):
        list.append(nested[i][0])
    return list

@app.route('/', methods=['GET'])
def bar_chart():
    result =df.groupBy('stars').count().sort("stars").cache()
    
    labels=result.select("stars").collect()
    labels=explode(labels)#flatMap(lambda x:x[0]).collect()


    values=result.select("count").collect()
    values=explode(values)
    print(labels)
    print(values)




    return render_template("chart.html", labels = labels, values = values)




