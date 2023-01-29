# import necessary libraries
import datetime
import six
import emoji
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import translate_v2 as translate
# since there were issues with relative path imports handle it
try:
    from .utils.runnerConfig import *
except ImportError:
    from utils.runnerConfig import *

# get the runtime arguments if specified and create pipeline arguments from them
parser = argparse.ArgumentParser()
path_args, pipeline_args = parser.parse_known_args()

def translate_text_gcp(target, text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    param target:str: The target language that the text should be translated/converted to
    param text:str: The text that requires conversion
    return result["translatedText"]:str: The translated text in English
    return result["detectedSourceLanguage"]:str: The original Source language that was detected by translation api
    """
    # initiate the translation client
    translate_client = translate.Client()
    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")

    # translate the tweet text and return the response
    result = translate_client.translate(text, target_language=target)
    return result["translatedText"], result["detectedSourceLanguage"]

def extract_tweet_data(dict_element):
    """This function is used to extract the tweet information from the original tweet data that was received.
    param dict_element:dict: The original data that was received from the twitter for the tweet information
    return data:list[dict]: The extracted tweet information
    """
    # print("data_dict", dict_element["data"], '\n')

    # extract the tweet information and return it
    data = dict_element["data"]["tweet_info"]
    data["id"] = str(data["id"])
    data["created_at"] = datetime.datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y').strftime('%d/%m/%Y %H:%M:%S')
    data["user_id"] = str(dict_element["data"]["user_info"]["id"])
    return data


def extract_user_data(dict_element):
    """This function is used to extract the tweet user information from the original received twitter data.
    param dict_element:dict: The original data that was received from the twitter for the user information
    return user:list[dict]: The extracted user information
    """
    user = dict_element["data"]["user_info"]
    user["id"] = str(user["id"])
    user["created_at"] = datetime.datetime.strptime(user["created_at"], '%a %b %d %H:%M:%S %z %Y').strftime('%d/%m/%Y %H:%M:%S')
    return user


def translate_tweet_text(dict_element):
    """This function is used to extract the tweet text information from the original received twitter data and translate it into English when needed.
    param dict_element:dict: The original data that was received from the twitter for the text information
    return dict: The extracted translation information
    """
    data = dict_element["data"]["tweet_info"]
    text = data["full_text"]
    translated_text, detected_original_lang = translate_text_gcp(translation_target_lang, text)
    return {"tweet_id": str(data["id"]), "originalText": text, "originalLang": detected_original_lang, "translatedText": translated_text, "translatedLang": translation_target_lang}


class ExtractEntitiesFromTweets(beam.DoFn):
    """This class is used to extract the tweet entities information from the original received twitter data.
    """
    def generate_entity_data(self, entity, entity_type, tweet_text=''):
        """This function is used to extract the tweet emojis information from the original received twitter text.
        param entity:dict: The entity information that was received from the twitter data
        param entity_type:str: The type of entity information that needs to be extracted
        param tweet_text:optional:str: The tweet text from which the emojis needs to be extracted from
        return str: The combined entity types data for each entity
            """
        entity_list = []
        if entity_type == 'emojis':
            emojis = emoji.emoji_list(tweet_text)
            for symbol in emojis:
                entity_list.append(symbol["emoji"])

        else:
            for data in entity[entity_type]:
                if entity_type == 'hashtags':
                    entity_list.append(data["text"])
                elif entity_type == 'symbols':
                    entity_list.append(data["text"])
                elif entity_type == 'user_mentions':
                    entity_list.append(data["screen_name"])
                elif entity_type == 'urls':
                    entity_list.append(data["url"])
        return ','.join(entity_list)

    def concat_entity_data(self, entities):
        """This function is used to concat the similar tweet entities information together.
        param entities:dict: The entity information that was received from the twitter data
        return dict: The combined entity types data for all entities
        """
        hashtags = self.generate_entity_data(entities, "hashtags")
        user_mentions = self.generate_entity_data(entities, "user_mentions")
        symbol = self.generate_entity_data(entities, "symbols")
        urls = self.generate_entity_data(entities, "urls")

        return {"hashtags": hashtags,
                "user_mentions": user_mentions,
                "symbol": symbol,
                "urls": urls}

    def concat_emojis_data(self, tweet_text):
        """This function is used to concat the emojis tweet entities information together.
        param tweet_text:str: The tweet text that contains all the emojis
        return dict: The combined emojis present in the tweet text
        """
        emojis = self.generate_entity_data(entity={}, tweet_text= tweet_text, entity_type="emojis")
        return {"emojis": emojis}

    def process(self, element):
        """This function is executed when we call the DoFn class. It is used to extract all the entities
        present in tweet information.
        param element:dict: The entities row that was received from the source
        return concatenate_entity_data:list[dict]: The combined entities such as hashtags, emojis, user mentions and symbols
         present in the tweet text
        """
        # get the tweet id and text
        tweet_id = str(element["data"]["tweet_info"]["id"])
        tweet_text = element["data"]["tweet_info"]["full_text"]

        # extract the respective entities
        entity_dict = self.concat_entity_data(element["data"]["entities_info"])
        emoji_dict = self.concat_emojis_data(tweet_text)
        entities_concat = {**entity_dict, **emoji_dict}
        entities_concat = (json.dumps(entities_concat)).encode("unicode_escape").decode("utf-8").replace("'","")
        concatenate_entity_data = {"tweet_id": tweet_id, "entitiesData": entities_concat}
        return [concatenate_entity_data]

def run_main(path_arguments, pipeline_arguments):
    """This function is is the entry point for the Beam pipeline. It is used to extract all the information and
    load respective data to the corresponding mysql and Bigquery tables.
    param path_arguments:args
    return pipeline_arguments:args: The arguments required for apache beam to run its pipeline
    """
    print("Starting the load job")

    # get the options based on the arguments that were provided
    options = PipelineOptions(pipeline_arguments)

    # set streaming to True for real time processing
    options.view_as(StandardOptions).streaming = True

    #create a Apache Beam Pipeline
    p = beam.Pipeline(options=options)

    # create a PCollection to hold the information that was received from the source.
    main_pipeline = (
        p
        | "Read from Pubsub-twitter feeds" >> beam.io.ReadFromPubSub(subscription=input_pubsub_subscription)
        | "Create fixed windows" >> beam.WindowInto(beam.window.FixedWindows(5))
        | "Cleanse data" >> beam.Map(lambda data: data.rstrip().lstrip())
        | "Convert to JSON" >> beam.Map(lambda msg: json.loads(msg.decode("utf-8")))
        )

    # extract user information from the original PCollection
    tweets_users_data = main_pipeline | "Extract User Info" >> beam.Map(extract_user_data)

    # write user information from the original PCollection to Mysql database
    (tweets_users_data
     | "write users to mysql" >> relational_db.Write(
                source_config=sink_db_config, table_config=db_tweet_user_table_config))

    # write user information from the original PCollection to Bigquery
    (tweets_users_data
     | "write users to BQ" >> beam.io.WriteToBigQuery(bq_tweets_users_table_spec, schema=bq_tweets_users_table_schema))

    # extract tweets information from the original PCollection
    tweets_data = main_pipeline | "Extract Tweets Info" >> beam.Map(extract_tweet_data)

    # write tweets information from the derived PCollection to Mysql database
    (tweets_data
    | "write tweets to mysql" >> relational_db.Write(
        source_config=sink_db_config, table_config=db_tweet_table_config))

    # write tweets information from the original PCollection to Bigquery
    (tweets_data
     | "write tweets to BQ" >> beam.io.WriteToBigQuery(bq_tweets_table_spec, schema=bq_tweets_table_schema))

    # extract translated information from the original PCollection
    translated_data = main_pipeline | "Extract translated Info" >> beam.Map(translate_tweet_text)

    # write translated information from the derived PCollection to Mysql database
    (translated_data
     | "write translated to mysql" >> relational_db.Write(
                source_config=sink_db_config, table_config=db_tweet_translation_table_config
            ))

    # write translated information from the original PCollection to Bigquery
    (translated_data
     | "write translated to BQ" >> beam.io.WriteToBigQuery(bq_tweets_translations_table_spec, schema=bq_tweets_translations_table_schema))

    # extract entities information from the original PCollection
    tweets_entities_data = main_pipeline | "Extract Entities Info" >> beam.ParDo(ExtractEntitiesFromTweets())

    # write entities information from the derived PCollection to Mysql database
    (tweets_entities_data | "write Entities to mysql" >> relational_db.Write(
                source_config=sink_db_config, table_config=db_tweet_entity_table_config
            ))

    # write entities information from the original PCollection to Bigquery
    (tweets_entities_data
     | "write entities to BQ" >> beam.io.WriteToBigQuery(bq_tweets_entities_table_spec, schema=bq_tweets_entities_table_schema))

    # start the pipeline in streaming mode
    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    # create the missing DB objects if any
    create_db_objects(mysql_host, int(mysql_port), mysql_username, mysql_password, mysql_database, mysql_ddl_file_path)

    # run the pipeline
    run_main(path_args, pipeline_args)
