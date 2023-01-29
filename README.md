# IKEA_Usecase

### A coding excerise to load the Twitter feeds about the conversations that is going on between the users about a given term.

#### Project Structure
![Images/img.png](img.png)

### Jobs directory contains the streaming python code to get the tweets from Twitter that contains specific terms and push it into pubsub.
Jobs/ApacheBeam/runner/runnerMain.py file has the Apache Beam code to load these tweets from pubsub to a mysql database for querying purposes.

#### Steps to Execute:

###### 1. Clone the repository to your local environment.
###### 2. Install the required packages by running pip install -r requirements.txt from project root directory
###### 3. Generate a JSON credential file for the GCP service account which has pubsub write role and save the file in the IKEA_Usecase>>ApacheBeam>>credentials>>gcpServiceAccounts directory
###### 4. Update the .env file with the proper environment values
###### 5. Open 2 CMD/Terminal windows.
###### 6. In one terminal/cmd window, run the IKEA_Usecase>>ApacheBeam>>Job>>Streaming>>TwitterFeedToPubSub>>twitterFeedsToPubsub.py file to start getting the tweet information in real time. Sample Screenshot below.
![Images/img_1.png](img_1.png)
###### 6. In another terminal run the IKEA_Usecase>>ApacheBeam>>Job>>Streaming>>ApacheBeam>>runner>>runnerMain.py file to load the data from pubsub to a mysql database. The database tables can be configured through file conf.json 