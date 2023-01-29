import tweepy
import time
from google.cloud import pubsub_v1
import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credentials_file_path

root_dir = os.path.dirname(os.path.abspath(__file__))
conf_file_path = os.path.join(root_dir, "../../../conf.json")
with open(conf_file_path) as json_data:
    configuration = json.load(json_data)

api_key = os.getenv("TWITTER_API_KEY")
api_secret_key = os.getenv("TWITTER_API_KEY_SECRET")
access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
gcp_project_id = os.getenv("GCP_PROJECT_ID")
gcp_pubsub_topic_id = os.getenv("PUBSUB_TOPIC_ID")


client = tweepy.Client(bearer_token, api_key, api_secret_key, access_token, access_token_secret)
auth = tweepy.OAuth1UserHandler(api_key, api_secret_key, access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)
search_terms = configuration["search_terms"]
publisher = pubsub_v1.PublisherClient()
print("topic details", gcp_pubsub_topic_id)
topic_path = publisher.topic_path(gcp_project_id, gcp_pubsub_topic_id)

class TweetStream(tweepy.StreamingClient):
    def on_connect(self):
        print("Connected")
        return "Connected"

    def on_data(self, raw_data):
        jsonData = json.loads(raw_data)
        try:
            tweet = (api.get_status(jsonData["data"]["id"], tweet_mode='extended'))._json
            print("tweet", tweet)
            pubsub_json_dict = {"data":{"tweet_info":{"created_at": tweet["created_at"],
                                              "id": tweet["id"],
                                              "retweet_count": tweet["retweet_count"],
                                              "favorite_count": tweet["favorite_count"],
                                              "favorited": tweet["favorited"],
                                              "retweeted": tweet["retweeted"],
                                              "lang": tweet["lang"],
                                              "full_text": tweet["full_text"]
                                              },
                                "user_info": {"id": tweet["user"]["id"],
                                              "name": tweet["user"]["name"],
                                              "screen_name": tweet["user"]["screen_name"],
                                              "location": tweet["user"]["location"],
                                              "description": tweet["user"]["description"],
                                              "protected": tweet["user"]["protected"],
                                              "followers_count": tweet["user"]["followers_count"],
                                              "listed_count": tweet["user"]["listed_count"],
                                              "created_at": tweet["user"]["created_at"],
                                              "favourites_count": tweet["user"]["favourites_count"],
                                              "geo_enabled": tweet["user"]["geo_enabled"],
                                              "verified": tweet["user"]["verified"],
                                              "lang": tweet["user"]["lang"],
                                              "has_extended_profile": tweet["user"]["has_extended_profile"]
                                              },
                                "place_info": {"geo": tweet["geo"],
                                               "coordinates": tweet["coordinates"],
                                               "place": tweet["place"]
                                               },
                                "entities_info": {"hashtags": tweet["entities"]["hashtags"],
                                             "symbols": tweet["entities"]["symbols"],
                                             "user_mentions": tweet["entities"]["user_mentions"],
                                             "urls": tweet["entities"]["urls"]
                                             }
                                },
                                "pubsub_event_datetime": datetime.datetime.now().strftime("%Y/%d/%m %H:%M:%S")}
            print("gcp_pubsub_topic_id", gcp_pubsub_topic_id, "pubsub_json_dict", pubsub_json_dict)
            publisher.publish(topic_path, json.dumps(pubsub_json_dict).encode("utf-8"), origin="Twitter API")
        except tweepy.errors.NotFound:
            print("Tweet Not Found for ID={}".format(jsonData["data"]["id"]))
        time.sleep(5)

    def on_error(self, status_code):
        if status_code == 420:
            print('Returning False in on_data disconnects the stream')
            return False
        elif status_code == 404:
            print('No status found with that ID')
            return False

stream = TweetStream(bearer_token=bearer_token)
for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))
stream.filter(tweet_fields=["id", "entities"], )
# place_fields=["contained_within","country","country_code","full_name","geo","id","name","place_type"]
#user_fields=["id","name","username","created_at","description","entities","location","verified"]

