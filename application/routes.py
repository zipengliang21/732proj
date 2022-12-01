from application import app,db

from flask import Flask, jsonify,render_template, flash, request, redirect, url_for, json
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
        business_dic['longitude'] = data['longitude']
        business_list.append(business_dic)
        business_dic = {}
    data = json.dumps(business_list)
    return render_template("map.html",data=data)



    




