# import necessary libraries
from beam_nuggets.io import relational_db
import json
from apache_beam.io.gcp.internal.clients import bigquery
from dotenv import load_dotenv
import pymysql
import os

# load environment variables from .env file
load_dotenv()

# load configuration file
root_dir = os.path.dirname(os.path.abspath(__file__))
conf_file_path = os.path.join(root_dir, "../../../../conf.json")
with open(conf_file_path) as json_data:
    configuration = json.load(json_data)

# get environment and configuration values and assign to vaiables
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_credentials_file_path
input_pubsub_subscription = os.getenv("PUBSUB_SUBSRCRIPTION_NAME")
gcp_project_id = os.getenv("GCP_PROJECT_ID")
gcp_pubsub_topic_id = os.getenv("PUBSUB_TOPIC_ID")
mysql_host = os.getenv("MYSQL_HOST")
mysql_port = int(os.getenv("MYSQL_PORT", 3306))
mysql_username = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_IKEA_DATABASE", '')
mysql_ddl_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), configuration["sinkProperties"]["ddl_statement_file_path"])
translation_target_lang = os.getenv("TRANSLATION_TARGET_LANGUAGE")


# get Bigquery dataset where the tweets should be written to
bq_dataset = configuration["bq_properties"]["tableProperties"]["tweetsTable"]["dataset"]

# get Bigquery table name and schema and write and create disposition to construct table specification for tweets table
bq_tweets_table_name = configuration["bq_properties"]["tableProperties"]["tweetsTable"]["tableName"]
bq_tweets_table_schema = configuration["bq_properties"]["tableProperties"]["tweetsTable"]["schema"]
bq_tweets_table_write_disposition = configuration["bq_properties"]["tableProperties"]["tweetsTable"]["writeDisposition"]
bq_tweets_table_create_disposition = configuration["bq_properties"]["tableProperties"]["tweetsTable"]["createDisposition"]
bq_tweets_table_spec = bigquery.TableReference(projectId=gcp_project_id, datasetId=bq_dataset, tableId=bq_tweets_table_name)

# get Bigquery table name and schema and write and create disposition to construct table specification for tweets translation table
bq_tweets_translations_table_name = configuration["bq_properties"]["tableProperties"]["tweetsTranslationTable"]["tableName"]
bq_tweets_translations_table_schema = configuration["bq_properties"]["tableProperties"]["tweetsTranslationTable"]["schema"]
bq_tweets_translations_table_write_disposition = configuration["bq_properties"]["tableProperties"]["tweetsTranslationTable"]["writeDisposition"]
bq_tweets_translations_table_create_disposition = configuration["bq_properties"]["tableProperties"]["tweetsTranslationTable"]["createDisposition"]
bq_tweets_translations_table_spec = bigquery.TableReference(projectId=gcp_project_id, datasetId=bq_dataset, tableId=bq_tweets_translations_table_name)

# get Bigquery table name and schema and write and create disposition to construct table specification for tweets users table
bq_tweets_users_table_name = configuration["bq_properties"]["tableProperties"]["tweetsUsers"]["tableName"]
bq_tweets_users_table_schema = configuration["bq_properties"]["tableProperties"]["tweetsUsers"]["schema"]
bq_tweets_users_table_write_disposition = configuration["bq_properties"]["tableProperties"]["tweetsUsers"]["writeDisposition"]
bq_tweets_users_table_create_disposition = configuration["bq_properties"]["tableProperties"]["tweetsUsers"]["createDisposition"]
bq_tweets_users_table_spec = bigquery.TableReference(projectId=gcp_project_id, datasetId=bq_dataset, tableId=bq_tweets_users_table_name)

# get Bigquery table name and schema and write and create disposition to construct table specification for tweets entities table
bq_tweets_entities_table_name = configuration["bq_properties"]["tableProperties"]["tweetsEntities"]["tableName"]
bq_tweets_entities_table_schema = configuration["bq_properties"]["tableProperties"]["tweetsEntities"]["schema"]
bq_tweets_entities_table_write_disposition = configuration["bq_properties"]["tableProperties"]["tweetsEntities"]["writeDisposition"]
bq_tweets_entities_table_create_disposition = configuration["bq_properties"]["tableProperties"]["tweetsEntities"]["createDisposition"]
bq_tweets_entities_table_spec = bigquery.TableReference(projectId=gcp_project_id, datasetId=bq_dataset, tableId=bq_tweets_entities_table_name)

# create Sink configuration for the Mysql database
sink_db_config = relational_db.SourceConfiguration(
    drivername=configuration["sinkProperties"]["databaseDriver"],
    host=mysql_host,
    port=mysql_port,
    username=mysql_username,
    password=mysql_password,
    database=mysql_database,
    create_if_missing=True if configuration["sinkProperties"]["createDbIfNotExist"] == 'true' else False
)

# create tweets table configuration for the Mysql database
db_tweet_table_config = relational_db.TableConfiguration(
    name=configuration["sinkProperties"]["tableProperties"]["tweetsTable"]["tableName"],
    create_if_missing=True if configuration["sinkProperties"]["tableProperties"]["tweetsTable"]["properties"]["create_if_missing"] == 'true' else False,
    primary_key_columns=configuration["sinkProperties"]["tableProperties"]["tweetsTable"]["properties"]["primary_key_columns"],
)

# create tweets translation table configuration for the Mysql database
db_tweet_translation_table_config = relational_db.TableConfiguration(
    name=configuration["sinkProperties"]["tableProperties"]["tweetsTranslationTable"]["tableName"],
    create_if_missing=True if configuration["sinkProperties"]["tableProperties"]["tweetsTranslationTable"]["properties"]["create_if_missing"] == 'true' else False,
    primary_key_columns=configuration["sinkProperties"]["tableProperties"]["tweetsTranslationTable"]["properties"]["primary_key_columns"],
)

# create tweets users table configuration for the Mysql database
db_tweet_user_table_config = relational_db.TableConfiguration(
    name=configuration["sinkProperties"]["tableProperties"]["tweetsUsers"]["tableName"],
    create_if_missing=True if configuration["sinkProperties"]["tableProperties"]["tweetsUsers"]["properties"]["create_if_missing"] == 'true' else False,
    primary_key_columns=configuration["sinkProperties"]["tableProperties"]["tweetsUsers"]["properties"]["primary_key_columns"],
)

# create tweets entities table configuration for the Mysql database
db_tweet_entity_table_config = relational_db.TableConfiguration(
    name=configuration["sinkProperties"]["tableProperties"]["tweetsEntities"]["tableName"],
    create_if_missing=True if configuration["sinkProperties"]["tableProperties"]["tweetsEntities"]["properties"]["create_if_missing"] == 'true' else False,
    primary_key_columns=configuration["sinkProperties"]["tableProperties"]["tweetsEntities"]["properties"]["primary_key_columns"],
)


def parse_sql(filename):
    """ This function is used to parse the .sql file for creating the database objects.
    param filename:String: This parameter accepts the file path of the .sql file
    return ddl_statements_list:list: This parameter is the extracted SQL queries that should be executed on the database at start up"""
    # open the sql file
    data = open(filename, 'r').readlines()
    ddl_statements_list = []
    delimiter = ';'
    ddl_statement = ''

    # iterate through the sql file and construct queries
    for lineno, line in enumerate(data):
        if not line.strip():
            continue

        # ignore if the line is a comment
        if line.startswith('--'):
            continue

        # for any other delimiter apart from ;
        if 'delimiter' in line:
            delimiter = line.split()[1]
            continue

        # contruct the sql statements with delimiter as ;
        if (delimiter not in line):
            ddl_statement += line.replace(delimiter, ';')
            continue

        if ddl_statement:
            ddl_statement += line
            ddl_statements_list.append(ddl_statement.strip())
            ddl_statement = ''
        else:
            ddl_statements_list.append(line.strip())
    return ddl_statements_list


def create_db_objects(in_mysql_host, in_mysql_port, in_mysql_username, in_mysql_password, in_mysql_database, in_ddl_file_path):
    """This function is used to create MYSQL database objects ar start up.
    param in_mysql_host:str: the host IP address of the mysql database
    param in_mysql_port:str: the port of the mysql database
    param in_mysql_username:str: the username to connect to the database
    param in_mysql_password:str: the password to connect to the database
    param in_mysql_database:str: the database name
    param in_ddl_file_path:str: the path of the sql file to be executed
    """
    # construct the pymysql connect object usign the credentials
    conn = pymysql.connect(host=in_mysql_host,
                           port=int(in_mysql_port),
                            user=in_mysql_username,
                            password = in_mysql_password,
                            db=in_mysql_database)

    # parse the sql file to extract individual queries from it
    ddl_statements_list = parse_sql(in_ddl_file_path)
    with conn.cursor() as cursor:
        # execute the sql queries
        for ddl_statement in ddl_statements_list:
            cursor.execute(ddl_statement)
        # commit and close the pymysql connection object
        conn.commit()
        conn.close()

if __name__ == '__main__':
    pass