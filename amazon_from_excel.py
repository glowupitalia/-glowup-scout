import os
import time
import urllib.parse
import requests
import pandas as pd


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


def search_by_ean(ean, access_token):
    endpoint = "https://sellingpartnerapi-eu.amazon.com"
    path = "/catalog/2022-04-01/items"

    params = {
        "marketplaceIds": os.environ["MARKETPLACE_ID"],
        "identifiers": str(ean),
        "identifiersType": "EAN",
        "includedData": "summaries,salesRanks,attributes,images",
    }

    url = endpoint + path + "?" + urllib.parse.urlencode(params)

    headers = {
        "x-amz-access-token": access_token,
        "Accept": "application/json",
    }

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        return {
            "EAN": ean,
            "Errore": f"{r.status_code} - {r.text}"
        }

    data = r.json()
    items = data.get("items", [])

    if not items:
        return {
            "EAN": ean,
            "Errore": "Nessun prodotto trovato"
        }

    item = items[0]
    summary = item.get("summaries", [{}])[0]

    bsr_beauty = ""
    bsr_categoria = ""
    categoria_bsr = ""

    sales_ranks = item.get("salesRanks", [])
    if sales_ranks:
        sr = sales_ranks[0]

        display = sr.get("displayGroupRanks", [])
        if display:
            bsr_beauty = display[0].get("rank", "")

        classification = sr.get("classificationRanks", [])
        if classification:
            bsr_categoria = classification[0].get("rank", "")
            categoria_bsr = classification[0].get("title", "")

    return {
        "EAN": ean,
        "ASIN": item.get("asin", ""),
        "Titolo": summary.get("itemName", ""),
        "Brand": summary.get("brand", ""),
        "Categoria": summary.get("browseClassification", {}).get("displayName", ""),
        "BSR Beauty": bsr_beauty,
        "BSR Categoria": bsr_categoria,
        "Categoria BSR": categoria_bsr,
        "Errore": ""
    }


def main():
    load_env()
    access_token = get_access_token()

    df = pd.read_excel("input_ean.xlsx", dtype={"EAN": str})

    results = []

    for ean in df["EAN"]:
        print(f"Analizzo EAN: {ean}")
        result = search_by_ean(ean, access_token)
        results.append(result)
        time.sleep(1)

    output = pd.DataFrame(results)
    output["EAN"] = output["EAN"].astype(str)

    output.to_excel("output_amazon.xlsx", index=False)

    print("Creato output_amazon.xlsx")


if __name__ == "__main__":
    main()
