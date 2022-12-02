from application import app,db

from flask import Flask, jsonify,render_template, flash, request, redirect, url_for, json
from bson import ObjectId
#from pyspark.sql import SparkSession, functions, types

#spark = SparkSession.builder.appName('jsfile').getOrCreate()
#spark.sparkContext.setLogLevel('WARN')
#sc = spark.sparkContext
#jsfile = 'ab.json'
#df = spark.read.json(jsfile).cache()





    
# def expand(nested):
#     list=[]
#     for i in range(9):
#         list.append(nested[i][0])
#     return list

@app.route('/', methods=['GET'])
def bar_chart():
    # result =df.groupBy('stars').count().sort("stars").cache()
    
    # labels=result.select("stars").collect()
    # labels=expand(labels)#flatMap(lambda x:x[0]).collect()


    # values=result.select("count").collect()
    #values=expand(values)
    return render_template("chart.html")#, labels = labels, values = values)




@app.route('/map', methods=['GET'])
def map():
    business = db.ab.find()
    business_dic = {}
    business_list = []
    for data in business:
        business_dic['name'] = data['name']
        business_dic['latitude'] = data['latitude']
        business_dic['stars'] = data['stars']
        business_dic['review_count'] = data['review_count'] 
        business_dic['longitude'] = data['longitude']
        business_list.append(business_dic)
        business_dic = {}
        data = json.dumps(business_list)
    return render_template("map.html",data=data)


@app.route('/map/<s>', methods=['GET'])
def star_map(s):
        business = db.ab.find({ 'stars': int(s) })
        business_dic = {}
        business_list = []
        for data in business:
            business_dic['name'] = data['name']
            business_dic['latitude'] = data['latitude']
            business_dic['stars'] = data['stars']
            business_dic['review_count'] = data['review_count'] 
            business_dic['longitude'] = data['longitude']
            business_list.append(business_dic)
            business_dic = {}
        data = json.dumps(business_list)
        return render_template("map.html",data=data)

@app.route('/location', methods=['GET'])
def location():
        out = db.out.find()
        out_dic = {}
        out_list = []
        for data in out:
            out_dic['name'] = data['store']
            out_dic['longitude'] = data['initial longitude']
            out_dic['latitude'] = data['initial latitude']
            out_list.append(out_dic)
            out_dic = {}
        data = json.dumps(out_list)
        
        return render_template("location.html",data=data)




    




