import requests
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch, helpers
# Define the API URL for the train_model endpoint https://1958-34-168-1-95.ngrok-free.app/
url = "https://ca02-35-247-55-123.ngrok-free.app/train_model"

# Load the Excel file
#excel_file = '/home/amin/Documents/hadoop_project/Projet_Big_Data/tesla_twitter_stock_data.xlsx'
#excel_file = '/shared_volume/Projet_Big_Data/tesla_twitter_stock_data.xlsx'
import pandas as pd
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch(["http://localhost:9200"])

# Query Elasticsearch to get Twitter data
response = es.search(
    index="twitter_data", 
    body={
        "query": {
            "match_all": {}
        },
        "_source": ["time","username", "followers", "tweet"]
    }
)

# Parse the response and extract data
tweets_data = []
for hit in response['hits']['hits']:
    tweet_info = hit['_source']
    tweets_data.append({
        "time": tweet_info['time'],
        "username": tweet_info['username'],
        "followers": tweet_info['followers'],
        "tweet": tweet_info['tweet']
    })

# Load the data into a pandas DataFrame
social_data = pd.DataFrame(tweets_data)

# Show the first few rows
print(social_data.head())

# Query Elasticsearch to get stock price data
response = es.search(
    index="stocks", 
    body={
        "query": {
            "match_all": {}
        },
        "_source": ["time", "stock_price"]
    }
)

# Parse the response and extract data
stock_data = []
for hit in response['hits']['hits']:
    stock_info = hit['_source']
    stock_data.append({
        "time": stock_info['time'],
        "stock_price": stock_info['stock_price']
    })

# Load the data into a pandas DataFrame
stock_data = pd.DataFrame(stock_data)


# Read the first sheet into a DataFrame
#social_data = pd.read_excel(excel_file, sheet_name='Twitter Data')
#social_data = social_data.drop('Sentiment', axis=1)
social_data.columns = social_data.columns.str.lower()
# Read the second sheet into a DataFrame
#stock_data = pd.read_excel(excel_file, sheet_name='Stock Prices')
stock_data.columns = stock_data.columns.str.lower()

# Convert DataFrame to JSON format that matches the request body
social_data_json = social_data.to_json(orient='split')
stock_data_json = stock_data.to_json(orient='split')



# Prepare the payload as a dictionary with stock_data as key
payload = {
    "stock_data": json.loads(stock_data_json),  # Convert the JSON string to a dictionary
    "social_data": json.loads(social_data_json)
}

# Send a POST request to the train_model endpoint
response = requests.post(url, json=payload)
#print(response.text.json()["complete_data"])
print(response.json())

data = response.json()
# Check the response from the server
if response.status_code == 200:
    print("Model training request was successful!")
def prepare_bulk_data(data):
    actions = []
    for item in data['complete_data']:
        action = {
            "_op_type": "index",
            "_index": "all_data_except_predictions",  # Replace with your actual index name
            "_source": {
                "time": item['time'],
                "stock_price": item['stock_price'],
                "username": item['username'],
                "followers": item['followers'],
                "tweet": item['tweet'],
                "sentiment_scores": item['sentiment_scores'],
                "sentiment": item['sentiment'],
                "weighted_sentiment": item['weighted_sentiment']
            }
        }
        actions.append(action)
    return actions

# Index the data using the bulk helper function
actions = prepare_bulk_data(data)

# Bulk indexing
helpers.bulk(es, actions)

print("Data indexed successfully!")  
