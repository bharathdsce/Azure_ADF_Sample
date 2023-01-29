import unittest
import random
from time import sleep
import os

from tweepy import *

"""Configurations"""
# Must supply twitter account credentials for tests
oauth_consumer_key = ''
oauth_consumer_secret = ''
oauth_token=''
oauth_token_secret=''

"""Unit tests"""


class TweepyAPITests(unittest.TestCase):
    def setUp(self):
        auth = OAuthHandler(oauth_consumer_key, oauth_consumer_secret)
        auth.set_access_token(oauth_token, oauth_token_secret)
        self.api = API(auth)
        self.api.retry_count = 2
        self.api.retry_delay = 5

    def testgetstatus(self):
        s = self.api.get_status(id=123)
        print(s)