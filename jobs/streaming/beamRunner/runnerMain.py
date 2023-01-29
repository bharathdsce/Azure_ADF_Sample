import datetime
import six
import emoji
import apache_beam as beam
import argparse
from beam_nuggets.io import relational_db
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
try:
    from .utils.runnerConfig import *
except ImportError:
    from utils.runnerConfig import *
from google.cloud import translate_v2 as translate
# (create_db_objects, input_pubsub_subscription, sink_db_config,
#                           db_tweet_table_config, db_tweet_translation_table_config,
#                           db_tweet_user_table_config, db_tweet_entity_table_config,
#                           mysql_host, mysql_port, mysql_username, mysql_password,
#                           mysql_database, mysql_ddl_file_path, bq_tweets_table_spec,
#                           bq_tweets_translations_table_spec, bq_tweets_users_table_spec,
#                           bq_tweets_entities_table_spec,bq_tweets_table_schema, bq_tweets_translations_table_schema,
#                           bq_tweets_users_table_schema, bq_tweets_entities_table_schema)
# from runnerConfig import *
import json

def translate_text_gcp(target, text):
    """Translates text into the target language.

    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """
    translate_client = translate.Client()
    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")
    result = translate_client.translate(text, target_language=target)
    return result["translatedText"], result["detectedSourceLanguage"]

def extract_tweet_data(dict_element):
    # print("data_dict", dict_element["data"], '\n')
    data = dict_element["data"]["tweet_info"]
    data["id"] = str(data["id"])
    data["created_at"] = datetime.datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y').strftime('%d/%m/%Y %H:%M:%S')
    data["user_id"] = str(dict_element["data"]["user_info"]["id"])
    return data


def extract_user_data(dict_element):
    user = dict_element["data"]["user_info"]
    user["id"] = str(user["id"])
    user["created_at"] = datetime.datetime.strptime(user["created_at"], '%a %b %d %H:%M:%S %z %Y').strftime('%d/%m/%Y %H:%M:%S')
    return user


def translate_tweet_text(dict_element):
    data = dict_element["data"]["tweet_info"]
    text = data["full_text"]
    translated_text, detected_original_lang = translate_text_gcp(translation_target_lang, text)
    return {"tweet_id": str(data["id"]), "originalText": text, "originalLang": detected_original_lang, "translatedText": translated_text, "translatedLang": translation_target_lang}


class ExtractEntitiesFromTweets(beam.DoFn):
    def generate_entity_data(self, entity, entity_type, tweet_text=''):
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
        hashtags = self.generate_entity_data(entities, "hashtags")
        user_mentions = self.generate_entity_data(entities, "user_mentions")
        symbol = self.generate_entity_data(entities, "symbols")
        urls = self.generate_entity_data(entities, "urls")

        return {"hashtags": hashtags,
                "user_mentions": user_mentions,
                "symbol": symbol,
                "urls": urls}

    def concat_emojis_data(self, tweet_text):
        emojis = self.generate_entity_data(entity={}, tweet_text= tweet_text, entity_type="emojis")
        return {"emojis": emojis}

    def process(self, element):
        tweet_id = str(element["data"]["tweet_info"]["id"])
        tweet_text = element["data"]["tweet_info"]["full_text"]

        entity_dict = self.concat_entity_data(element["data"]["entities_info"])
        emoji_dict = self.concat_emojis_data(tweet_text)
        entities_concat = {**entity_dict, **emoji_dict}
        entities_concat = (json.dumps(entities_concat)).encode("unicode_escape").decode("utf-8").replace("'","")
        concatenate_entity_data = {"tweet_id": tweet_id, "entitiesData": entities_concat}
        return [concatenate_entity_data]

parser = argparse.ArgumentParser()
path_args, pipeline_args = parser.parse_known_args()

def run_main(path_arguments, pipeline_arguments):
    print("Starting the load job")

    options = PipelineOptions(pipeline_arguments)
    options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=options)

    main_pipeline = (
        p
        | "Read from Pubsub-twitter feeds" >> beam.io.ReadFromPubSub(subscription=input_pubsub_subscription)
        # | "Create fixed windows" >> beam.WindowInto(beam.window.FixedWindows(5))
        | "Cleanse data" >> beam.Map(lambda data: data.rstrip().lstrip())
        | "Convert to JSON" >> beam.Map(lambda msg: json.loads(msg.decode("utf-8")))
        )

    tweets_users_data = main_pipeline | "Extract User Info" >> beam.Map(extract_user_data)

    (tweets_users_data
     | "write users to mysql" >> relational_db.Write(
                source_config=sink_db_config, table_config=db_tweet_user_table_config))

    (tweets_users_data
     | "write users to BQ" >> beam.io.WriteToBigQuery(bq_tweets_users_table_spec, schema=bq_tweets_users_table_schema))

    tweets_data = main_pipeline | "Extract Tweets Info" >> beam.Map(extract_tweet_data)

    (tweets_data
    | "write tweets to mysql" >> relational_db.Write(
        source_config=sink_db_config, table_config=db_tweet_table_config))

    (tweets_data
     | "write tweets to BQ" >> beam.io.WriteToBigQuery(bq_tweets_table_spec, schema=bq_tweets_table_schema))

    translated_data = main_pipeline | "Extract translated Info" >> beam.Map(translate_tweet_text)

    (translated_data
     | "write translated to mysql" >> relational_db.Write(
                source_config=sink_db_config, table_config=db_tweet_translation_table_config
            ))

    (translated_data
     | "write translated to BQ" >> beam.io.WriteToBigQuery(bq_tweets_translations_table_spec, schema=bq_tweets_translations_table_schema))

    tweets_entities_data = main_pipeline | "Extract Entities Info" >> beam.ParDo(ExtractEntitiesFromTweets())

    (tweets_entities_data | "write Entities to mysql" >> relational_db.Write(
                source_config=sink_db_config, table_config=db_tweet_entity_table_config
            ))

    (tweets_entities_data
     | "write entities to BQ" >> beam.io.WriteToBigQuery(bq_tweets_entities_table_spec, schema=bq_tweets_entities_table_schema))

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    create_db_objects(mysql_host, int(mysql_port), mysql_username, mysql_password, mysql_database, mysql_ddl_file_path)
    run_main(path_args, pipeline_args)
