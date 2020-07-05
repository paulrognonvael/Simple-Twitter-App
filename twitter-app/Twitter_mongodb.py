#!/usr/bin/env python
# coding: utf-8

import json
import pymongo

# Enter your keys/secrets as strings in the following fields
credentials = {}
credentials['CONSUMER_KEY'] = "xxxxxxxx"
credentials['CONSUMER_SECRET'] = "xxxxxxxx"
credentials['ACCESS_TOKEN'] = "xxxxxxxx"
credentials['ACCESS_SECRET'] = "xxxxxxxx"

# create a MongoClient 
from pymongo import MongoClient

myclient = MongoClient('localhost', 27017)

mydb = myclient["twitter_corona"]

mycol = mydb["tweets"]


from twython import TwythonStreamer
import json

# Filter out unwanted data
def process_tweet(tweet):
    d = {}
    d['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    d['user'] = tweet['user']
    d['created_at']=tweet['created_at']
    d['geo']=tweet['geo']
    d['reply_count']=tweet['reply_count']
    d['retweet_count']=tweet['retweet_count']
    d['favorite_count']=tweet['favorite_count']
    d['id']=tweet['id_str']
    d['in_reply_to_status_id']=tweet['in_reply_to_status_id_str']
    d['in_reply_to_user_id_str']=tweet['in_reply_to_user_id_str']
    return d
    
    
# Create a class that inherits TwythonStreamer
class MyStreamer(TwythonStreamer):     

    count=0
    # Received data
    def on_success(self, data):

        # Only collect tweets in English
        if data['lang'] == 'en':
            tweet_data = process_tweet(data)
            self.save_to_mongodb(data)
            self.count+=1
            if(self.count%100==0):
                print("tweet received: "+str(self.count))
            

    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()
        
    # Save each tweet to mongodb
    def save_to_mongodb(self, tweet):
        t = json.dumps(tweet)
        loaded_entry = json.loads(t)
        mycol.insert_one(loaded_entry)


# Instantiate from our streaming class
stream = MyStreamer(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'], 
                    credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'])
# Start the stream
stream.statuses.filter(track='corona')




