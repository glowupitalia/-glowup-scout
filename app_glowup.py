import logging
import os
import time
import urllib.parse
import requests
import streamlit as st
from PIL import Image
import pandas as pd

from batch_analysis import (
    analyze_products,
    decision_from_score,
    opportunity_score,
    summarize_results,
    write_results_excel,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



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


def read_input_excel(uploaded_file):
    source_file = getattr(uploaded_file, "name", "<uploaded file>")
    logger.info("READING EXCEL | file=%s", source_file)
    try:
        return pd.read_excel(uploaded_file, dtype={"EAN": str})
    except Exception:
        logger.exception(
            "INPUT EXCEL READ FAILED | phase=READING EXCEL file=%s",
            source_file,
        )
        raise


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
                logger.exception(
                    "SINGLE PRODUCT ANALYSIS FAILED | "
                    "phase=PROCESSING PRODUCT ean=%s",
                    ean.strip(),
                )
                st.error(f"Errore: {e}")

# --- ANALISI EXCEL MULTIPLA ---

st.divider()
st.header("📊 Analisi multipla da Excel")

uploaded_file = st.file_uploader("Carica un file Excel con colonna EAN", type=["xlsx"])

if uploaded_file:
    df_input = read_input_excel(uploaded_file)

    if "EAN" not in df_input.columns:
        st.error("Il file deve contenere una colonna chiamata EAN.")
    else:
        costo_col = None
        for col in df_input.columns:
            if str(col).strip().lower() == "costo":
                costo_col = col

        st.write(f"EAN trovati: {len(df_input)}")

        if st.button("Analizza Excel"):
            source_file = getattr(uploaded_file, "name", "<uploaded file>")
            output_file = "glowup_scout_output.xlsx"
            started_at = time.monotonic()
            phase = "START ANALYSIS"
            logger.info(
                "START ANALYSIS | products=%s file=%s",
                len(df_input),
                source_file,
            )

            progress_widget = None
            progress_callback = None
            try:
                progress_widget = st.progress(0)
                progress_callback = progress_widget.progress
            except Exception:
                logger.exception(
                    "PROGRESS INITIALIZATION FAILED | phase=START ANALYSIS "
                    "file=%s; continuing without UI progress",
                    source_file,
                )

            try:
                token = get_access_token()
                phase = "PROCESSING PRODUCTS"
                df_results = analyze_products(
                    df_input=df_input,
                    costo_col=costo_col,
                    token=token,
                    search_catalog=search_catalog,
                    search_pricing=search_pricing,
                    safe_call=safe_call,
                    progress_callback=progress_callback,
                    source_file=source_file,
                )

                phase = "ANALYSIS COMPLETED"
                logger.info(
                    "ANALYSIS COMPLETED | products=%s file=%s",
                    len(df_results),
                    source_file,
                )

                phase = "WRITING EXCEL"
                generated_file = write_results_excel(df_results, output_file)
                duration_seconds = time.monotonic() - started_at
                result_summary = summarize_results(df_results)

                phase = "READY FOR DOWNLOAD"
                logger.info(
                    "READY FOR DOWNLOAD | file=%s duration_seconds=%.2f",
                    generated_file,
                    duration_seconds,
                )
            except Exception:
                logger.exception(
                    "BATCH ANALYSIS FAILED | phase=%s file=%s output_file=%s",
                    phase,
                    source_file,
                    output_file,
                )
                st.error(
                    f"Errore durante la fase '{phase}'. "
                    "Consulta i log per i dettagli."
                )
            else:
                try:
                    if progress_widget is not None:
                        progress_widget.empty()
                    total_col, eligible_col, not_eligible_col, duration_col = (
                        st.columns(4)
                    )
                    total_col.metric(
                        "Prodotti analizzati",
                        result_summary["total"],
                    )
                    eligible_col.metric(
                        "Prodotti idonei",
                        result_summary["eligible"],
                    )
                    not_eligible_col.metric(
                        "Prodotti non idonei",
                        result_summary["not_eligible"],
                    )
                    duration_col.metric(
                        "Durata elaborazione",
                        f"{duration_seconds:.1f} s",
                    )

                    with open(generated_file, "rb") as output:
                        st.download_button(
                            label="📥 Scarica risultato Excel",
                            data=output,
                            file_name="glowup_scout_output.xlsx",
                            mime=(
                                "application/vnd.openxmlformats-officedocument."
                                "spreadsheetml.sheet"
                            ),
                            on_click="ignore",
                        )
                except Exception:
                    logger.exception(
                        "RESULT UI FAILED | phase=READY FOR DOWNLOAD "
                        "file=%s output_file=%s",
                        source_file,
                        generated_file,
                    )
