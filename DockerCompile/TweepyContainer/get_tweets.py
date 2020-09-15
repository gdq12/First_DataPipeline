import config
import time
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import json
import logging
from pymongo import MongoClient
from datetime import datetime

client=MongoClient(host='my_mongodb', port=27017)
db=client.twitter
tweets=db.tweets

def authenticate():
    """
    To create authentication token for twitter.
    """
    auth = OAuthHandler(config.CONSUMER_API_KEY, config.CONSUMER_API_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

    return auth

class TwitterListener(StreamListener):
    '''
    Retrieve json based on filter from Twitter API, store specified "chunks"
    to MonogoDB.
    '''
    def on_data(self, data):
        t = json.loads(data)
        logging.warning('---------------Incoming Tweet!!!!!!---------------')

        hashtags=[]

        if t['created_at'] != None:
            try:
                timeStamp=t['created_at']
            except KeyError:
                timeStamp='Not provided'

        if t['user']['name'] != None:
            try:
                user=t['user']['name']
            except KeyError:
                user='Not provided'

        if t['user']['screen_name'] != None:
            try:
                username=t['user']['screen_name']
            except KeyError:
                username='Not provided'

        try:
            location=t['user']['location']
        except KeyError:
            location='Not provided'

        if 'extended_tweet' in t:
            for hashtag in t['extended_tweet']['entities']['hashtags']:
                hashtags.append(hashtag['text'])
        elif 'hashtags' in t['entities'] and len(t['entities']['hashtags']) > 0:
            hashtags=[item['text'] for item in t['entities']['hashtags']]

        if 'extended_tweet' in t:
            text=t['extended_tweet']['full_text']
        else:
            text=t['text']

        tweet = {'User time':timeStamp, 'tweepy time':datetime.utcnow(),\
        'user':user, 'username':username, 'location':location,\
        'hashtags':hashtags,'text':text}

        tweets.insert_many([tweet])
        logging.critical('Successfully added to mongoDB!!!!!!!')
        logging.warning('-------------------------------------')

    def on_error(self, status):
        if status == 420:
            print(status)
            return False

    def on_limit(self, status):
        print('Rate Limit Exceeded, sleep for 6 minutes')
        time.sleep(6*60)
        return True

if __name__ == '__main__':

    auth = authenticate()
    listener = TwitterListener()
    stream = Stream(auth, listener)
    while True:
        try:
            stream.filter(track=['berlin'], languages=['en'], stall_warnings=True)
        except:
            logging.critical('---Problem with streamer, reinitiate in 10 seconds---')
            time.sleep(10)
            continue
