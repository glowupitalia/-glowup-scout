import os
import time
import urllib.parse
import requests
import streamlit as st
from PIL import Image
import pandas as pd


def decision_from_score(score):
    try:
        score = int(score)
    except Exception:
        return "Da verificare"

    if score >= 85:
        return "Compra"
    elif score >= 65:
        return "Valuta bene"
    elif score >= 45:
        return "Monitorare"
    else:
        return "Evita"



def safe_call(func, *args, retries=3, wait=2):
    last_error = None

    for attempt in range(retries):
        try:
            return func(*args)
        except requests.exceptions.HTTPError as e:
            last_error = e
            status = e.response.status_code if e.response is not None else None

            if status == 429:
                time.sleep(wait * (attempt + 1))
                continue

            raise
        except Exception as e:
            last_error = e
            time.sleep(wait * (attempt + 1))

    raise last_error


def load_env():
    try:
        with open(".env", "r") as f:
            for line in f:
                if "=" in line:
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value
    except FileNotFoundError:
        pass


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


def money(obj):
    if not obj:
        return ""
    return f"{obj.get('Amount', '')} {obj.get('CurrencyCode', '')}"


def search_catalog(ean, token):
    endpoint = "https://sellingpartnerapi-eu.amazon.com"
    path = "/catalog/2022-04-01/items"

    params = {
        "marketplaceIds": os.environ["MARKETPLACE_ID"],
        "identifiers": ean,
        "identifiersType": "EAN",
        "includedData": "summaries,salesRanks,images",
    }

    url = endpoint + path + "?" + urllib.parse.urlencode(params)

    headers = {
        "x-amz-access-token": token,
        "Accept": "application/json",
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    data = r.json()
    items = data.get("items", [])

    if not items:
        return None

    item = items[0]
    summary = item.get("summaries", [{}])[0]

    image_url = ""
    images = item.get("images", [])
    if images:
        img_list = images[0].get("images", [])
        if img_list:
            image_url = img_list[0].get("link", "")

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
        "Immagine": image_url,
    }


def search_pricing(asin, token):
    url = f"https://sellingpartnerapi-eu.amazon.com/products/pricing/v0/items/{asin}/offers"

    params = {
        "MarketplaceId": os.environ["MARKETPLACE_ID"],
        "ItemCondition": "New"
    }

    headers = {
        "x-amz-access-token": token,
        "Accept": "application/json",
    }

    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()

    data = r.json()
    payload = data.get("payload", {})

    summary = payload.get("Summary", {})
    offers = payload.get("Offers", [])

    buy_box = ""
    buybox_prices = summary.get("BuyBoxPrices", [])
    if buybox_prices:
        buy_box = money(buybox_prices[0].get("LandedPrice"))

    lowest_fba = ""
    lowest_fbm = ""

    for lp in summary.get("LowestPrices", []):
        channel = lp.get("fulfillmentChannel", "")
        price = money(lp.get("LandedPrice"))
        if channel == "Amazon":
            lowest_fba = price
        elif channel == "Merchant":
            lowest_fbm = price

    fba_count = 0
    fbm_count = 0
    offer_rows = []

    for offer in offers:
        is_fba = offer.get("IsFulfilledByAmazon", False)
        if is_fba:
            fba_count += 1
        else:
            fbm_count += 1

        price = offer.get("ListingPrice", {})
        shipping = offer.get("Shipping", {})
        feedback = offer.get("SellerFeedbackRating", {})
        seller_id = offer.get("SellerId", "")

        total_price = ""
        try:
            total_amount = float(price.get("Amount", 0)) + float(shipping.get("Amount", 0))
            total_price = f"{round(total_amount, 2)} {price.get('CurrencyCode', '')}"
        except Exception:
            total_price = ""

        offer_rows.append({
            "Seller ID": seller_id,
            "FBA": "SI" if is_fba else "NO",
            "Buy Box": "SI" if offer.get("IsBuyBoxWinner") else "NO",
            "Prezzo": money(price),
            "Spedizione": money(shipping),
            "Totale": total_price,
            "Feedback %": feedback.get("SellerPositiveFeedbackRating", ""),
            "Prime": "SI" if offer.get("PrimeInformation", {}).get("IsPrime") else "NO",
        })

    return {
        "Buy Box": buy_box,
        "Prezzo minimo FBA": lowest_fba,
        "Prezzo minimo FBM": lowest_fbm,
        "Venditori totali": len(offers),
        "Venditori FBA": fba_count,
        "Venditori FBM": fbm_count,
        "Offerte": offer_rows,
    }



def to_int(value):
    try:
        if value in ["", None, "None"]:
            return None
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return None


def to_int(value):
    try:
        if value in ["", None, "None"]:
            return None
        return int(float(str(value).replace(",", ".")))
    except Exception:
        return None


def opportunity_score(bsr_beauty, venditori_totali):
    score = 0

    bsr = to_int(bsr_beauty)
    sellers = to_int(venditori_totali)

    # 70% velocità di vendita
    if bsr is not None:
        if bsr <= 1000:
            score += 70
        elif bsr <= 5000:
            score += 60
        elif bsr <= 10000:
            score += 50
        elif bsr <= 25000:
            score += 35
        elif bsr <= 50000:
            score += 20
        else:
            score += 10

    # 30% concorrenza
    if sellers is not None:
        if sellers <= 3:
            score += 30
        elif sellers <= 6:
            score += 20
        elif sellers <= 10:
            score += 10

    if score >= 85:
        return score, "🟢 Eccellente"
    elif score >= 65:
        return score, "🟢 Ottima"
    elif score >= 45:
        return score, "🟡 Interessante"
    elif score >= 25:
        return score, "🟠 Da valutare"
    else:
        return score, "🔴 Debole"



def decision_from_score(score):
    try:
        score = int(score)
    except Exception:
        return "Da verificare"

    if score >= 85:
        return "Compra"
    elif score >= 65:
        return "Valuta bene"
    elif score >= 45:
        return "Monitorare"
    else:
        return "Evita"


load_env()

st.set_page_config(page_title="GlowUp Product Scout", layout="wide")

logo = Image.open("glowup-italia-signature-transparent.png")

st.image(logo, width=350)

st.title("Product Scout")
st.write("Analisi prodotto Amazon da EAN: BSR, immagine, Buy Box e offerte venditori.")

ean = st.text_input("Inserisci EAN prodotto")

if st.button("Analizza EAN"):
    if not ean:
        st.warning("Inserisci prima un EAN.")
    else:
        with st.spinner("Analisi Amazon in corso..."):
            try:
                token = get_access_token()
                catalog = search_catalog(ean.strip(), token)

                if catalog is None:
                    st.error("Nessun prodotto trovato.")
                else:
                    pricing = safe_call(search_pricing, catalog["ASIN"], token)

                    st.success("Prodotto trovato!")

                    left, right = st.columns([1, 2])

                    with left:
                        if catalog["Immagine"]:
                            st.image(catalog["Immagine"], width=280)
                        st.metric("ASIN", catalog["ASIN"])
                        st.metric("Brand", catalog["Brand"])

                        asin = catalog["ASIN"]
                        st.link_button("🔗 Apri scheda Amazon", f"https://www.amazon.it/dp/{asin}")
                        st.link_button("🛒 Apri offerte venditori", f"https://www.amazon.it/gp/offer-listing/{asin}")

                    with right:
                        c1, c2, c3 = st.columns(3)
                        c1.metric("BSR Beauty", catalog["BSR Beauty"])
                        c2.metric("Venditori FBA", pricing["Venditori FBA"])
                        c3.metric("Buy Box", pricing["Buy Box"])

                        c4, c5, c6 = st.columns(3)
                        c4.metric("Venditori totali", pricing["Venditori totali"])
                        c5.metric("Venditori FBA", pricing["Venditori FBA"])
                        c6.metric("Venditori FBM", pricing["Venditori FBM"])

                        c7, c8, c9 = st.columns(3)
                        c7.metric("Prezzo min FBA", pricing["Prezzo minimo FBA"])
                        c8.metric("Prezzo min FBM", pricing["Prezzo minimo FBM"])
                        c9.metric("Categoria", catalog["Categoria"])

                    st.subheader("Titolo prodotto")
                    st.write(catalog["Titolo"])

                    st.subheader("Categoria")
                    st.write(catalog["Categoria"])

                    st.subheader("Offerte venditori")
                    if pricing["Offerte"]:
                        st.dataframe(pd.DataFrame(pricing["Offerte"]), width="stretch")
                    else:
                        st.info("Nessuna offerta disponibile.")

                    st.subheader("Dati riepilogo")
                    summary = {
                        **{k: v for k, v in catalog.items() if k != "Immagine"},
                        **{k: v for k, v in pricing.items() if k != "Offerte"},
                    }
                    st.dataframe([summary], width="stretch")

            except Exception as e:
                st.error(f"Errore: {e}")

# --- ANALISI EXCEL MULTIPLA ---

st.divider()
st.header("📊 Analisi multipla da Excel")

uploaded_file = st.file_uploader("Carica un file Excel con colonna EAN", type=["xlsx"])

if uploaded_file:
    df_input = pd.read_excel(uploaded_file, dtype={"EAN": str})

    if "EAN" not in df_input.columns:
        st.error("Il file deve contenere una colonna chiamata EAN.")
    else:
        costo_col = None
        for col in df_input.columns:
            if str(col).strip().lower() == "costo":
                costo_col = col

        st.write(f"EAN trovati: {len(df_input)}")

        if st.button("Analizza Excel"):
            results = []
            progress = st.progress(0)
            token = get_access_token()

            for i, row in df_input.iterrows():
                ean_value = str(row["EAN"]).strip()
                costo_value = ""
                if costo_col:
                    costo_value = row[costo_col]

                try:
                    catalog = safe_call(search_catalog, ean_value, token)

                    if catalog:
                        pricing = safe_call(search_pricing, catalog["ASIN"], token)

                        results.append({
                            "EAN": ean_value,
                            "Costo": costo_value,
                            "ASIN": catalog["ASIN"],
                            "Titolo": catalog["Titolo"],
                            "Brand": catalog["Brand"],
                            "Categoria": catalog["Categoria"],
                            "BSR Beauty": catalog["BSR Beauty"],
                            "Buy Box": pricing["Buy Box"],
                            "Venditori totali": pricing["Venditori totali"],
                            "Venditori FBA": pricing["Venditori FBA"],
                            "Venditori FBM": pricing["Venditori FBM"],
                            "Prezzo minimo FBA": pricing["Prezzo minimo FBA"],
                            "Prezzo minimo FBM": pricing["Prezzo minimo FBM"],
                            "Score": opportunity_score(catalog["BSR Beauty"], pricing["Venditori totali"])[0],
                            "Opportunità": opportunity_score(catalog["BSR Beauty"], pricing["Venditori totali"])[1],
                            "Decisione": decision_from_score(opportunity_score(catalog["BSR Beauty"], pricing["Venditori totali"])[0]),
                            "Link Amazon": f"https://www.amazon.it/dp/{catalog['ASIN']}",
                            "Link Offerte": f"https://www.amazon.it/gp/offer-listing/{catalog['ASIN']}",
                            "Stato": "TROVATO",
                            "Errore": ""
                        })
                    else:
                        results.append({"EAN": ean_value, "Costo": costo_value, "Stato": "NON TROVATO SU AMAZON", "Stato": "TROVATO",
                            "Errore": ""})

                except Exception as e:
                    results.append({
                        "EAN": ean_value,
                        "Costo": costo_value,
                        "Stato": "ERRORE API / LIMITE AMAZON",
                        "Errore": str(e)
                    })

                progress.progress((i + 1) / len(df_input))
                time.sleep(0.7)

            df_results = pd.DataFrame(results)

            if "Venditori totali" in df_results.columns:
                for idx in df_results.index:

                    asin = str(df_results.at[idx, "ASIN"]) if "ASIN" in df_results.columns else ""
                    venditori = df_results.at[idx, "Venditori totali"]

                    if asin in ["", "None", "nan"]:
                        df_results.at[idx, "Stato"] = "NON TROVATO SU AMAZON"

                    elif pd.isna(venditori) or venditori == 0:
                        df_results.at[idx, "Stato"] = "TROVATO SENZA OFFERTE"

                    else:
                        df_results.at[idx, "Stato"] = "TROVATO CON OFFERTE"
            if "Score" in df_results.columns:
                df_results["Score"] = pd.to_numeric(df_results["Score"], errors="coerce").fillna(0)
                df_results["BSR Beauty"] = pd.to_numeric(df_results["BSR Beauty"], errors="coerce")
                df_results = df_results.sort_values(
                    by=["Score", "BSR Beauty"],
                    ascending=[False, True],
                    na_position="last"
                )

            st.success("Analisi completata!")
            st.dataframe(df_results, width="stretch")

            output_file = "glowup_scout_output.xlsx"

            with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
                df_results.to_excel(writer, index=False, sheet_name="Risultati")

                ws = writer.sheets["Risultati"]

                headers = {}
                for col in range(1, ws.max_column + 1):
                    headers[ws.cell(row=1, column=col).value] = col

                amazon_col = headers.get("Link Amazon")
                offerte_col = headers.get("Link Offerte")

                for row in range(2, ws.max_row + 1):
                    if amazon_col:
                        cell = ws.cell(row=row, column=amazon_col)
                        if cell.value:
                            cell.hyperlink = cell.value
                            cell.style = "Hyperlink"

                    if offerte_col:
                        cell = ws.cell(row=row, column=offerte_col)
                        if cell.value:
                            cell.hyperlink = cell.value
                            cell.style = "Hyperlink"

            with open(output_file, "rb") as f:
                st.download_button(
                    label="📥 Scarica risultato Excel",
                    data=f,
                    file_name="glowup_scout_output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
