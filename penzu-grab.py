import os
from dotenv import load_dotenv
import requests
from requests_oauthlib import OAuth1
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# Configure API endpoint and MongoDB connection
API_URL = os.getenv('API_URL')
MONGO_URI = os.getenv('MONGO_URI')
DATABASE_NAME = "penzu"
COLLECTION_NAME = "entries"

# OAuth credentials
OAUTH_CONSUMER_KEY = os.getenv('OAUTH_CONSUMER_KEY')
OAUTH_CONSUMER_SECRET = os.getenv('OAUTH_CONSUMER_SECRET')
OAUTH_TOKEN = os.getenv('OAUTH_TOKEN')
OAUTH_TOKEN_SECRET = os.getenv('OAUTH_TOKEN_SECRET')

# Initialize MongoDB client and collection
def init_db():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    return collection

# Fetch journal entries from API with OAuth
def fetch_entries():
    page = 1
    auth = OAuth1(OAUTH_CONSUMER_KEY, OAUTH_CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

    while True:
        print(f"Fetching page {page}...")
        response = requests.get(API_URL, params={"limit": 10, "order": "cad", "page": page}, auth=auth, verify=False)
        if response.status_code != 200:
            print(f"Error fetching page {page}: {response.status_code}")
            break

        data = response.json()
        entries = data.get("entries", [])

        if not entries:
            break

        for entry in entries:
            fetch_and_save_entry(entry["entry"]["id"])

        if not data.get("entries") or len(entries) < 10:
            break

        page += 1

def fetch_and_save_entry(entry_id):
    auth = OAuth1(OAUTH_CONSUMER_KEY, OAUTH_CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    entry_url = f"{API_URL}/{entry_id}"
    print(f"Fetching entry {entry_id}...")
    response = requests.get(entry_url, auth=auth, verify=False)

    if response.status_code != 200:
        print(f"Error fetching entry {entry_id}: {response.status_code}")
        return

    entry_data = response.json()
    save_entry_to_db(entry_data)

# Persist a single entry to MongoDB
def save_entry_to_db(entry):
    entry = entry["entry"]
    print(f"Saving entry {entry['id']}...")
    collection = init_db()
    collection.update_one(
        {"id": entry["id"]},
        {"$set": entry},
        upsert=True
    )

# Main execution flow
def main():
    fetch_entries()

if __name__ == "__main__":
    main()
