import os
import requests
import urllib.parse

def load_env():
    with open(".env", "r") as f:
        for line in f:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

def get_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": os.environ["LWA_REFRESH_TOKEN"],
        "client_id": os.environ["LWA_CLIENT_ID"],
        "client_secret": os.environ["LWA_CLIENT_SECRET"],
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

load_env()

ean = "8809562192770"
marketplace_id = os.environ["MARKETPLACE_ID"]
access_token = get_access_token()

endpoint = "https://sellingpartnerapi-eu.amazon.com"
path = "/catalog/2022-04-01/items"

params = {
    "marketplaceIds": marketplace_id,
    "identifiers": ean,
    "identifiersType": "EAN",
    "includedData": "summaries,salesRanks,attributes,images",
}

url = endpoint + path + "?" + urllib.parse.urlencode(params)

headers = {
    "x-amz-access-token": access_token,
    "Accept": "application/json",
}

response = requests.get(url, headers=headers)

print(response.status_code)

data = response.json()

for item in data.get("items", []):

    print("ASIN:", item.get("asin"))

    summaries = item.get("summaries", [])

    if summaries:

        s = summaries[0]

        print("Titolo:", s.get("itemName"))

        print("Brand:", s.get("brand"))

    print("Sales ranks:", item.get("salesRanks"))
