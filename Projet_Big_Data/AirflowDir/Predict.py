import requests
import json
import pandas as pd
from elasticsearch import Elasticsearch,helpers


# Define the API URL for the train_model endpoint https://0412-35-247-55-123.ngrok-free.app/
url = "https://ca02-35-247-55-123.ngrok-free.app/predict"

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
        "time":tweet_info['time'],
        "username": tweet_info['username'],
        "followers": tweet_info['followers'],
        "tweet": tweet_info['tweet']
    })

# Load the data into a pandas DataFrame
social_data = pd.DataFrame(tweets_data)

# Show the first few rows
print(social_data.head())

# Read the first sheet into a DataFrame
#social_data = pd.read_excel(excel_file, sheet_name='Twitter Data')
#social_data = social_data.drop('Sentiment', axis=1)
social_data.columns = social_data.columns.str.lower()

social_data['time'] = pd.to_datetime(social_data['time'], format='%d-%m-%Y')
social_data_sorted = social_data.sort_values(by='time', ascending=False)
social_data = social_data_sorted.head(32)


# Convert DataFrame to JSON format that matches the request body
social_data_json = social_data.to_json(orient='split')
# Prepare the payload as a dictionary with stock_data as key
payload = {
    "social_data": json.loads(social_data_json)
}

# Send a POST request to the train_model endpoint
response = requests.post(url, json=payload)
print("here")
# Check the response from the server
if response.status_code == 200:
    print("Model Prediction request was successful!")
    print(response.json())

response_data = response.json()  # Extract JSON data directly from the Response object
predictions = response_data["predictions"]

# Prepare the bulk data format
actions = []
for prediction in predictions:
    action = {
        "_op_type": "index",  # Operation type (indexing new document)
        "_index": "predictions",  # Index name
        "_source": {
            "date": prediction['date'],
            "predicted_stock_price": prediction['predicted_stock_price']
        }
    }
    actions.append(action)

# Bulk index the data
try:
    helpers.bulk(es, actions)
    print("Data indexed successfully.")
except Exception as e:
    print(f"Error indexing data: {e}")
