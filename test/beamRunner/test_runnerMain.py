from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import apache_beam as beam
import unittest
import json
from jobs.streaming.beamRunner.runnerMain import extract_tweet_data, extract_user_data, translate_tweet_text, translate_text_gcp, ExtractEntitiesFromTweets
from TestData.output.sample_ouput import *
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "ikeausecase.json"
class TweetDataTest(unittest.TestCase):
    def test_tweet_data(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "TestData/input/testJsonData.json"), "r") as f:
            test_data = [json.load(f)]
        with TestPipeline() as p:
            input_data = p | beam.Create(test_data)
            tweet_data_output = (input_data | beam.Map(extract_tweet_data))
            assert_that(tweet_data_output, equal_to(extractTweetDataOutput), label="Verify Tweet Output")

    def test_user_data(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "TestData/input/testJsonData.json"), "r") as f:
            test_data = [json.load(f)]
        with TestPipeline() as p:
            input_data = p | beam.Create(test_data)
            user_data_output = input_data | beam.Map(extract_user_data)
            assert_that(user_data_output, equal_to(extractUserDataOutput), label="Verify User Output")

    def test_translated_tweets(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "TestData/input/testJsonData.json"), "r") as f:
            test_data = [json.load(f)]
        with TestPipeline() as p:
            input_data = p | beam.Create(test_data)
            translated_output = input_data | beam.Map(translate_tweet_text)
            assert_that(translated_output, equal_to(translateTweetTextOutput), label="Verify Translated Tweet Output")

    def test_translated_text(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "TestData/input/testJsonData.json"), "r") as f:
            test_data = (json.load(f))["data"]["tweet_info"]["full_text"]
        translated_text, detected_lang = translate_text_gcp('en', test_data)
        self.assertEqual(translated_text, test_data, msg="Verify Translated Text Output")
        self.assertEqual(detected_lang, 'en', msg="Verify Detected Language Output")

    def test_entities_tweets(self):
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "TestData/input/testJsonData.json"), "r") as f:
            test_data = [json.load(f)]
        with TestPipeline() as p:
            input_data = p | beam.Create(test_data)
            entities_output = input_data | beam.ParDo(ExtractEntitiesFromTweets())
        assert_that(entities_output, equal_to(extractEntitiesFromTweetsOutput), label="Verify Entities Output")

if __name__ == "__main__":
    unittest.main()