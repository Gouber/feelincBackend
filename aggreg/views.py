from django.http import HttpResponse
from pymongo import MongoClient
import json as simplejson
from bson.json_util import dumps
import dateutil.parser
import datetime
from kafka import KafkaConsumer


def live(request):
    return HttpResponse("Hello world, you are live!")


def liveClear(request):
    print("This is entered")
    #consume all from the live topic
    consumer = KafkaConsumer('liveTopic',auto_offset_reset="latest", bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)
    for msg in consumer:
        print(msg)

    return HttpResponse("Hello, world. You're at the polls index.")


def map_helper(x):
    entry = {}
    sent = x["sentiment_analysis"]
    pos = sent[0]["score"]
    neg = sent[1]["score"]
    neutr = sent[2]["score"]
    max_score = max(pos,neg,neutr)
    if(max_score == pos):
        #it was pos
        entry["sentiment"] = 1
    elif(max_score == neg):
        entry["sentiment"] = -1
        pass
    else:
        entry["sentiment"] = 0
    entry["created_at"] = x["created_at"]
    return entry


def index(request):
    start_date = request.GET.get('start','')
    end_date = request.GET.get('end','')
    tag = str(request.GET.get('tag',''))
    start_date_parsed = ''
    end_date_parsed = ''
    if(start_date != '' and end_date != ''):
        start_date_parsed = dateutil.parser.parse(start_date)
        end_date_parsed = dateutil.parser.parse(end_date)
        print(start_date_parsed)
        print(end_date_parsed)
    a = MongoClient("mongodb://127.0.0.1:27017")
    db = a.tweets
    collection = db['tweets_nlp_ed']
    cursor = None
    if start_date_parsed != '' and end_date_parsed  != '' and tag != '':
        cursor = list(collection.find({"cashtags.tag": {"$in": [tag.lower(), tag.upper()]},"created_at": {'$lt': end_date_parsed, "$gte": start_date_parsed}}, {"created_at":1,  "sentiment_analysis": 1, "_id": 0}))
    elif start_date_parsed != '' and end_date_parsed  != '':
        cursor = list(collection.find({"created_at": {'$lt': end_date_parsed, "$gte": start_date_parsed}},  {"created_at":1, "sentiment_analysis": 1, "_id": 0}))
    elif tag != '':
        cursor = list(collection.find({"cashtags.tag": {"$in": [tag.lower(), tag.upper()]}}, {"created_at":1, "sentiment_analysis": 1, "_id": 0}))
    else:
        cursor = list(collection.find({}, {"created_at":1, "sentiment_analysis": 1, "_id": 0}))
    cursor_mapped = list(map(map_helper, cursor))

    dates = {}
    for x in cursor_mapped:
        sent = int(x["sentiment"])
        d = x["created_at"]
        date = str(datetime.datetime(d.year, d.month, d.day))
        if date in dates:
            #Just add one more to an already existing entry
            score_now = dates[date][sent]
            dates[date][sent] = score_now + 1
        else:
            dates[date] = {-1: 0, 0: 0, 1: 0}
            dates[date][sent] = 1

    data_to_return = []
    another_data_to_return = []
    final_thing_to_return = []

    for key in dates:
        entry = dates[key]
        obj = {}
        obj["name"] = key
        obj["neg"] = entry[-1]
        obj["pos"] = entry[1]
        obj["neutr"] = entry[0]
        data_to_return.append(obj)

        second_obj = {}
        second_obj["name"] = key
        second_obj["val"] = entry[1] - entry[-1]
        another_data_to_return.append(second_obj)

        third_obj = {}
        third_obj["name"] = key
        third_obj["val"] = (entry[1] - entry[-1]) * (entry[1] + entry[-1] + entry[0])
        final_thing_to_return.append(third_obj)

    resp = {"data": dumps(data_to_return), "second_data": dumps(another_data_to_return), "third_data": dumps(final_thing_to_return)}
    return HttpResponse(simplejson.dumps(resp))
