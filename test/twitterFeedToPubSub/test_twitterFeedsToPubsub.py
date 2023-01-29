import unittest
import tweepy
import os
from tweepy import *
from dotenv import load_dotenv
load_dotenv()

"""Configurations"""
api_key = os.getenv("TWITTER_API_KEY")
api_secret_key = os.getenv("TWITTER_API_KEY_SECRET")
access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")

class TweepyAPITests(unittest.TestCase):
    def setUp(self):
        auth = tweepy.OAuth1UserHandler(api_key, api_secret_key, access_token, access_token_secret)
        self.api = API(auth)
        self.api.retry_count = 2
        self.api.retry_delay = 5

    def test_get_user(self):
        user_info = self.api.get_user(screen_name='kp_bharath')._json
        user_fullname = user_info["name"]

        self.assertEqual(user_fullname, 'Bharath KP')

    def test_get_tweet(self):
        tweet_id = "1619434990139502592"
        tweet_info = (self.api.get_status(tweet_id, tweet_mode='extended'))._json
        tweet_created_dt = tweet_info["created_at"]
        self.assertEqual(tweet_created_dt, 'Sat Jan 28 20:39:23 +0000 2023')

    def test_get_trend(self):
        trend_list = self.api.available_trends()
        self.assertIsNotNone(trend_list)

    def test_geo_apis(self):
        geo_location_data = self.api.geo_id(place_id='c3f37afa9efcf94b')
        self.assertEqual(geo_location_data.name, 'Austin')

class TweepyCursorTests(unittest.TestCase):
    def setUp(self):
        auth = tweepy.OAuth1UserHandler(api_key, api_secret_key, access_token, access_token_secret)
        self.api = API(auth)
        self.api.retry_count = 2
        self.api.retry_delay = 5

    def testpagecursoritems(self):
        items = Cursor(self.api.user_timeline).items(1)
        item_list = []
        for item in items:
            item_list.append(item)
        self.assert_(len(item_list) > 0)
