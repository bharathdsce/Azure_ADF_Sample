# import necessary libraries
import tweepy
import time
from google.cloud import pubsub_v1
import datetime
import json
import os
from dotenv import load_dotenv

# load environment variabled from the .env file
load_dotenv()
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credentials_file_path

# load the configuration values form the conf file
root_dir = os.path.dirname(os.path.abspath(__file__))
conf_file_path = os.path.join(root_dir, "../../../conf.json")
with open(conf_file_path) as json_data:
    configuration = json.load(json_data)

# extract environment variables and assign to related variables
api_key = os.getenv("TWITTER_API_KEY")
api_secret_key = os.getenv("TWITTER_API_KEY_SECRET")
access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
gcp_project_id = os.getenv("GCP_PROJECT_ID")
gcp_pubsub_topic_id = os.getenv("PUBSUB_TOPIC_ID")

# Create a tweepy Twitter client object with authentication keys
client = tweepy.Client(bearer_token, api_key, api_secret_key, access_token, access_token_secret)
auth = tweepy.OAuth1UserHandler(api_key, api_secret_key, access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# get the search terms that we need to filter the tweets for
search_terms = configuration["search_terms"]

#initialize the pubsub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(gcp_project_id, gcp_pubsub_topic_id)


class TweetStream(tweepy.StreamingClient):
    """This class is used to fetch the Twitter streams that match the search terms and curate the data before sending
    it to PubSub in real time. This class inherits the tweepy.StreamingClient parent class"""
    def on_connect(self):
        """This function is used to check if the connection is established between the client and the twitter backend"""
        print("Connected")
        return "Connected"

    def on_data(self, raw_data):
        """This function is called when the service receives any new tweets that are being posted in the Twitter that
        matches the search conditon we have specified in the search terms. If any tweets are posted, we curate the data to
        extract only the required information before proceeding to send it to PubSub.
        param: raw_data:str: The raw data that is received from the Tweepy service on new tweet detection"""
        # convert the incoming raw data to json object
        jsonData = json.loads(raw_data)
        try:
            # get all the relevent information from twitter based on the tweet id which is unique for individual tweets
            tweet = (api.get_status(jsonData["data"]["id"], tweet_mode='extended'))._json
            # print("tweet", tweet)

            # create the dict object that needs to be sent to PubSub
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
            # print("gcp_pubsub_topic_id", gcp_pubsub_topic_id, "pubsub_json_dict", pubsub_json_dict)
            # publish the bytes version of the JSON created above to PubSub topic
            publisher.publish(topic_path, json.dumps(pubsub_json_dict).encode("utf-8"), origin="Twitter API")
        except tweepy.errors.NotFound:
            print("Tweet Not Found for ID={}".format(jsonData["data"]["id"]))
        time.sleep(5)

    def on_error(self, status_code):
        """This function is used to handle any exceptions that we may see while getting the tweet information"""
        if status_code == 420:
            print('Returning False in on_data disconnects the stream')
            return False
        elif status_code == 404:
            print('No status found with that ID')
            return False

# Create the stream object from the TweetStream class to start the streaming process
stream = TweetStream(bearer_token=bearer_token)

# add the search term as rules to the tweepy object so we fetch only those tweets that we are intersted in
for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))

# a handle to let tweepy know to always fetch the ids and entities of the tweet
stream.filter(tweet_fields=["id", "entities"], )

