import logging
import html
import json
import os
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import requests
import streamlit as st
from PIL import Image
import pandas as pd

from batch_analysis import (
    analyze_products,
    select_reference_price,
    summarize_results,
    write_results_excel,
)
from product_fees import search_product_fees_batch
from discovery import (
    DiscoveryCheckpointStore,
    LEGACY_CHECKPOINT_MESSAGE,
    default_filters,
    discovery_funnel_view,
    normalize_discovery_state,
)
from discovery_amazon import (
    RefreshingTokenProvider,
    correlate_catalog_items,
    get_item_offers_batch,
    parse_item_offers_batch,
    search_catalog_by_gtins_batch,
)
from direct_lookup import (
    direct_scenario_rows,
    format_eur,
    format_percent,
    run_direct_lookup,
    scenario_stock_availability,
)
from purchase_scenarios import (
    recommended_combination,
    recommended_scenario,
    scenario_requirement_label,
    target_price,
)
from supplier_preparation import SUPPORTED_SUPPLIERS
from supplier_catalog import SupplierCatalogStore, canonical_gtin14
from discovery_rotation import DiscoveryRotationStore
from discovery_jobs import DiscoveryJobRegistry
from discovery_ui import (
    discovery_phase_eta_seconds,
    discovery_phase_progress,
    format_eta,
    format_phase_steps,
)
from notifications import EmailConfig, NotificationOutbox
from discovery_freshness import AmazonFreshnessPolicy, POLICY_VERSION
from discovery_freshness import DiscoveryAmazonCache
from discovery_incremental import DiscoveryIncrementalStore
from discovery_taxonomy import (
    BEAUTY_PARENT_IDS,
    QOGITA_CATEGORY_TREE,
    default_qogita_category_filter,
)


DISCOVERY_OPERATIONAL_SUPPLIERS = ("qogita", "umma", "abw", "qudo")


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


def money_amount(obj):
    if not isinstance(obj, dict):
        return None
    try:
        amount = float(obj.get("Amount"))
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def _first_marketplace_record(values):
    if isinstance(values, list):
        marketplace_id = os.environ.get("MARKETPLACE_ID")
        return next(
            (
                value
                for value in values
                if isinstance(value, dict)
                and value.get("marketplaceId") in (None, marketplace_id)
            ),
            next((value for value in values if isinstance(value, dict)), {}),
        )
    return values if isinstance(values, dict) else {}


def _first_attribute(attributes, *names):
    if not isinstance(attributes, dict):
        return None
    for name in names:
        value = attributes.get(name)
        if isinstance(value, list) and value:
            return value[0]
        if value not in (None, "", []):
            return value
    return None


def extract_logistics(item):
    dimensions_payload = item.get("dimensions") or []
    dimensions = _first_marketplace_record(dimensions_payload)
    attributes = item.get("attributes") or {}
    product_type_record = _first_marketplace_record(item.get("productTypes") or [])

    item_dimensions = dimensions.get("item")
    package_dimensions = dimensions.get("package")
    item_weight = (
        item_dimensions.get("weight")
        if isinstance(item_dimensions, dict)
        else None
    )
    package_weight = (
        package_dimensions.get("weight")
        if isinstance(package_dimensions, dict)
        else None
    )

    return {
        "Peso prodotto": item_weight or _first_attribute(
            attributes,
            "item_weight",
            "item_weight_without_packaging",
        ),
        "Peso package": package_weight or _first_attribute(
            attributes,
            "item_package_weight",
            "package_weight",
        ),
        "Dimensioni prodotto": item_dimensions or _first_attribute(
            attributes,
            "item_dimensions",
        ),
        "Dimensioni package": package_dimensions or _first_attribute(
            attributes,
            "item_package_dimensions",
            "package_dimensions",
        ),
        "Product Type": product_type_record.get("productType", ""),
        "_Catalog attributes": attributes,
        "_Catalog dimensions": dimensions_payload,
    }


def search_catalog(ean, token):
    endpoint = "https://sellingpartnerapi-eu.amazon.com"
    path = "/catalog/2022-04-01/items"

    params = {
        "marketplaceIds": os.environ["MARKETPLACE_ID"],
        "identifiers": ean,
        "identifiersType": "EAN",
        "includedData": (
            "summaries,salesRanks,images,dimensions,attributes,productTypes"
        ),
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
        **extract_logistics(item),
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
    buy_box_amount = None
    buybox_prices = summary.get("BuyBoxPrices", [])
    if buybox_prices:
        landed_price = buybox_prices[0].get("LandedPrice")
        buy_box = money(landed_price)
        buy_box_amount = money_amount(landed_price)

    lowest_fba_amounts = []
    lowest_fbm_amounts = []

    for lp in summary.get("LowestPrices", []):
        channel = lp.get("fulfillmentChannel", "")
        price = money_amount(lp.get("LandedPrice"))
        if price is None:
            continue
        if channel == "Amazon":
            lowest_fba_amounts.append(price)
        elif channel == "Merchant":
            lowest_fbm_amounts.append(price)

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
            listing_amount = float(price.get("Amount", 0))
            shipping_amount = float(shipping.get("Amount", 0))
            total_amount = listing_amount + shipping_amount
            total_price = f"{round(total_amount, 2)} {price.get('CurrencyCode', '')}"
            if listing_amount > 0 and shipping_amount >= 0 and total_amount > 0:
                if is_fba:
                    lowest_fba_amounts.append(total_amount)
                else:
                    lowest_fbm_amounts.append(total_amount)
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

    lowest_fba_amount = min(lowest_fba_amounts, default=None)
    lowest_fbm_amount = min(lowest_fbm_amounts, default=None)

    return {
        "Buy Box": buy_box,
        "Buy Box Amount": buy_box_amount,
        "Prezzo minimo FBA": (
            f"{lowest_fba_amount} EUR" if lowest_fba_amount is not None else ""
        ),
        "Prezzo minimo FBA Amount": lowest_fba_amount,
        "Prezzo minimo FBM": (
            f"{lowest_fbm_amount} EUR" if lowest_fbm_amount is not None else ""
        ),
        "Prezzo minimo FBM Amount": lowest_fbm_amount,
        "Venditori totali": len(offers),
        "Venditori FBA": fba_count,
        "Venditori FBM": fbm_count,
        "Offerte": offer_rows,
    }


def apply_apple_ui():
    st.markdown("""
        <style>
        :root { color-scheme: light; }
        html, body, [class*="st-"] {
            font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display",
                "SF Pro Text", sans-serif;
        }
        .stApp { background: #f0f2f7; color: #121419; }
        .block-container {
            max-width: 1160px; padding-top: 1.25rem; padding-bottom: 3rem;
        }
        [data-testid="stHeader"] { background: transparent; }
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: #ffffff; border: .8px solid rgba(0,0,0,.055) !important;
            border-radius: 26px; box-shadow: 0 5px 12px rgba(0,0,0,.07);
        }
        [data-testid="stVerticalBlockBorderWrapper"] > div {
            padding: .05rem .2rem;
        }
        h1, h2, h3 { color: rgba(0,0,0,.90); letter-spacing: -.025em; }
        h1 {
            font-size: clamp(1.75rem, 3vw, 2.15rem); font-weight: 720;
            line-height: 1.08; margin: 0;
        }
        h2 { font-size: 1.28rem; font-weight: 700; margin-top: 0; }
        p { color: rgba(0,0,0,.54); }
        .gu-header { margin: 0 0 1rem; }
        .gu-subtitle {
            color: rgba(0,0,0,.54); font-size: .98rem;
            font-weight: 550; margin: .3rem 0 0;
        }
        .gu-product-brand {
            color: rgba(0,0,0,.52); font-size: .78rem; font-weight: 650;
            letter-spacing: .035em; text-transform: uppercase;
        }
        .gu-product-title {
            color: rgba(0,0,0,.88); font-size: 1.28rem; font-weight: 650;
            line-height: 1.25; margin: .3rem 0 .55rem;
        }
        .gu-meta { color: rgba(0,0,0,.50); font-size: .84rem; line-height: 1.55; }
        .gu-alert {
            border: 1px solid; border-radius: 12px; font-size: .9rem;
            margin: .25rem 0 1rem; padding: .7rem .9rem;
        }
        .gu-success { background: #f0f9f3; border-color: #ccebd5; color: #246b3d; }
        .gu-warning { background: #fff9e8; border-color: #f3e4ad; color: #7b5b00; }
        .gu-error { background: #fff2f1; border-color: #f3cfcb; color: #9f2d25; }
        [data-testid="stMetric"] {
            background: rgba(248,249,251,.92);
            border: 1px solid rgba(0,0,0,.055);
            border-radius: 16px; padding: .7rem .8rem;
        }
        [data-testid="stMetricLabel"] {
            color: rgba(0,0,0,.50); font-size: .76rem; font-weight: 650;
        }
        [data-testid="stMetricValue"] {
            color: rgba(0,0,0,.88); font-size: 1.22rem; font-weight: 720;
        }
        [data-testid="stTextInput"] input {
            background: #ffffff; border-color: rgba(0,0,0,.13);
            border-radius: 11px; color: rgba(0,0,0,.88);
            height: 2.65rem;
        }
        [data-testid="stTextInput"] input:focus {
            border-color: rgba(0,0,0,.62);
            box-shadow: 0 0 0 1px rgba(0,0,0,.20);
        }
        .stButton > button, .stDownloadButton > button,
        [data-testid="stLinkButton"] a,
        [data-testid="stFileUploaderDropzone"] button {
            border-radius: 11px; height: 2.65rem; min-height: 2.65rem;
            padding: 0 1rem; font-weight: 650;
            transition: transform .12s ease, background .12s ease;
        }
        .stButton > button[kind="primary"],
        .stDownloadButton > button[kind="primary"],
        [data-testid="stFileUploaderDropzone"] button {
            background: rgb(12% 39% 94%);
            border-color: rgb(12% 39% 94%);
            color: #ffffff;
        }
        .stButton > button[kind="primary"] *,
        .stDownloadButton > button[kind="primary"] *,
        [data-testid="stFileUploaderDropzone"] button * {
            color: #ffffff; font-weight: 650;
        }
        [data-testid="stLinkButton"] a {
            background: rgb(12% 39% 94%);
            border-color: rgb(12% 39% 94%);
            color: #ffffff;
        }
        [data-testid="stLinkButton"] a * {
            color: #ffffff; font-weight: 650;
        }
        .stButton > button[kind="primary"]:hover,
        .stDownloadButton > button[kind="primary"]:hover,
        [data-testid="stFileUploaderDropzone"] button:hover {
            background: rgb(12% 39% 94%);
            border-color: rgb(12% 39% 94%);
            color: #ffffff; filter: brightness(.88);
        }
        [data-testid="stLinkButton"] a:hover {
            background: rgb(12% 39% 94%);
            border-color: rgb(12% 39% 94%);
            color: #ffffff; filter: brightness(.88);
        }
        [data-testid="stLinkButton"] a:focus-visible {
            outline: 3px solid rgb(12% 39% 94% / 32%);
            outline-offset: 2px;
        }
        [data-testid="stLinkButton"] a[aria-disabled="true"] {
            background: rgb(12% 39% 94% / 42%);
            border-color: transparent; color: rgb(100% 100% 100% / 82%);
            pointer-events: none;
        }
        .stButton > button[kind="primary"]:focus-visible,
        .stDownloadButton > button[kind="primary"]:focus-visible,
        [data-testid="stFileUploaderDropzone"] button:focus-visible {
            outline: 3px solid rgb(12% 39% 94% / 32%);
            outline-offset: 2px;
        }
        .stButton > button[kind="primary"]:disabled,
        .stDownloadButton > button[kind="primary"]:disabled,
        [data-testid="stFileUploaderDropzone"] button:disabled {
            background: rgb(12% 39% 94% / 42%) !important;
            border-color: transparent !important;
            color: rgb(100% 100% 100% / 82%) !important; opacity: 1;
        }
        .stButton > button:hover, .stDownloadButton > button:hover,
        [data-testid="stFileUploaderDropzone"] button:hover,
        [data-testid="stLinkButton"] a:hover { transform: translateY(-1px); }
        [data-testid="stFileUploaderDropzone"] {
            background: #f8f9fb; border: 1px dashed rgba(0,0,0,.18);
            border-radius: 16px; box-sizing: border-box;
            height: 3.25rem; min-height: 3.25rem;
            padding: .3rem .4rem .3rem .9rem;
            gap: .75rem; align-items: center;
        }
        [data-testid="stFileUploaderDropzone"] button {
            width: 10.5rem; min-width: 10.5rem; max-width: 10.5rem;
            white-space: nowrap; box-sizing: border-box;
        }
        .gu-field-label {
            color: rgba(0,0,0,.78); font-size: .875rem; font-weight: 600;
            margin: 0 0 .35rem;
        }
        .st-key-ean_operation_row {
            background: #ffffff; border: 1px solid rgba(0,0,0,.13);
            border-radius: 16px; box-sizing: border-box;
            height: 3.25rem; min-height: 3.25rem;
            padding: .3rem .4rem .3rem .9rem;
        }
        .st-key-ean_operation_row [data-testid="stHorizontalBlock"] {
            align-items: center; gap: .75rem; height: 100%;
        }
        .st-key-ean_operation_row [data-testid="stTextInput"] {
            flex: 1 1 auto; min-width: 0;
        }
        .st-key-ean_operation_row [data-testid="stTextInput"] > div,
        .st-key-ean_operation_row [data-baseweb="input"],
        .st-key-ean_operation_row [data-baseweb="base-input"] {
            height: 2.65rem; border: 0; background: transparent;
            background-color: transparent !important;
            box-shadow: none !important; outline: none !important;
        }
        .st-key-ean_operation_row [data-testid="stTextInput"] input {
            border: 0 !important;
            background: transparent !important;
            background-color: transparent !important;
            box-shadow: none !important; outline: none !important;
            padding-left: 0;
        }
        .st-key-ean_operation_row:focus-within {
            border-color: rgb(12% 39% 94% / 55%);
            box-shadow: 0 0 0 2px rgb(12% 39% 94% / 14%);
        }
        .st-key-ean_operation_row [data-testid="stButton"] {
            flex: 0 0 10.5rem; width: 10.5rem;
            min-width: 10.5rem; max-width: 10.5rem;
        }
        .st-key-ean_operation_row [data-testid="stButton"] button {
            width: 10.5rem; min-width: 10.5rem; max-width: 10.5rem;
            box-sizing: border-box;
        }
        .st-key-product_amazon_actions [data-testid="stHorizontalBlock"] {
            gap: .75rem; align-items: center;
        }
        .st-key-product_amazon_actions [data-testid="stLinkButton"] {
            flex: 1 1 0; min-width: 0; width: 100%;
        }
        .st-key-product_amazon_actions [data-testid="stLinkButton"] a {
            width: 100%; box-sizing: border-box;
        }
        .st-key-back_single_home button,
        .st-key-back_batch_home button {
            background: transparent; border: 1px solid rgba(0,0,0,.12);
            color: rgba(0,0,0,.68); box-shadow: none;
        }
        .st-key-back_single_home button *,
        .st-key-back_batch_home button * {
            color: rgba(0,0,0,.68); font-weight: 650;
        }
        .st-key-back_single_home button:hover,
        .st-key-back_batch_home button:hover {
            background: rgba(255,255,255,.72);
            border-color: rgba(0,0,0,.2); filter: none;
        }
        [data-testid="stProgressBar"] > div { background: rgba(0,0,0,.09); }
        [data-testid="stProgressBar"] > div > div { background: #333840; }
        a { color: #333840; }
        [data-testid="stImage"] img { border-radius: 16px; object-fit: contain; }
        hr { border-color: rgba(0,0,0,.08); }
        @media (max-width: 700px) {
            .block-container { padding: .8rem .85rem 2rem; }
            [data-testid="stVerticalBlockBorderWrapper"] {
                border-radius: 20px; box-shadow: 0 3px 9px rgba(0,0,0,.055);
            }
            .gu-product-title { font-size: 1.15rem; }
            [data-testid="stMetricValue"] { font-size: 1.1rem; }
            .stButton > button, .stDownloadButton > button,
            [data-testid="stLinkButton"] a { width: 100%; }
            .st-key-ean_operation_row,
            [data-testid="stFileUploaderDropzone"] {
                height: 6.5rem; min-height: 6.5rem;
                padding: .4rem; gap: .4rem;
            }
            .st-key-ean_operation_row [data-testid="stHorizontalBlock"],
            [data-testid="stFileUploaderDropzone"] {
                flex-direction: column; align-items: stretch;
            }
            .st-key-product_amazon_actions [data-testid="stHorizontalBlock"] {
                flex-direction: column; align-items: stretch; gap: .5rem;
            }
            .st-key-ean_operation_row [data-testid="stButton"],
            .st-key-ean_operation_row [data-testid="stButton"] button,
            [data-testid="stFileUploaderDropzone"] button {
                width: 100%; min-width: 100%; max-width: 100%;
                flex-basis: 2.65rem;
            }
        }
        </style>
    """, unsafe_allow_html=True)


def ui_alert(message, kind="success"):
    safe_message = html.escape(str(message))
    st.markdown(
        f'<div class="gu-alert gu-{kind}">{safe_message}</div>',
        unsafe_allow_html=True,
    )


def display_value(value, fallback="—"):
    return fallback if value in (None, "", "None") else value


def single_price_label(price_source):
    return {
        "buy_box": "Buy Box",
        "min_fba": "Prezzo minimo FBA",
        "min_fbm": "Prezzo minimo FBM",
        "missing_price": "Prezzo non disponibile",
    }.get(price_source, "Prezzo non disponibile")


load_env()
st.set_page_config(page_title="GlowUp Product Scout", layout="wide")
apply_apple_ui()

logo = Image.open("glowup-italia-signature-transparent.png")
header_logo, header_copy = st.columns([1, 5], vertical_alignment="center")
with header_logo:
    st.image(logo, width=145)
with header_copy:
    st.markdown("""
        <div class="gu-header">
            <h1>Product Scout</h1>
            <p class="gu-subtitle">Trova e valuta nuove opportunità su Amazon</p>
        </div>
    """, unsafe_allow_html=True)


def return_to_home():
    st.session_state["ui_state"] = "home"
    st.session_state.pop("single_product_result", None)
    st.session_state.pop("single_status", None)
    st.session_state.pop("single_message", None)
    st.session_state.pop("batch_result", None)
    st.session_state.pop("batch_error", None)
    st.session_state.pop("discovery_result", None)
    st.session_state.pop("discovery_error", None)


def discovery_filter_error(filters, selected_suppliers):
    if not selected_suppliers:
        return "Seleziona almeno un fornitore."
    if int(filters["bsr_min"]) < 0:
        return "Il BSR minimo non può essere negativo."
    if int(filters["bsr_max"]) <= int(filters["bsr_min"]):
        return "Il BSR massimo deve essere maggiore del BSR minimo."
    if int(filters["max_fba_sellers"]) < 0 or int(filters["max_total_sellers"]) < 0:
        return "Il numero massimo di venditori non può essere negativo."
    if not 0 <= int(filters["minimum_margin"]) <= 100:
        return "Il margine minimo deve essere compreso tra 0% e 100%."
    return None


def _toggle_all_discovery_suppliers():
    value = bool(st.session_state.get("discovery_supplier_all"))
    for supplier in DISCOVERY_OPERATIONAL_SUPPLIERS:
        available = supplier != "qogita" or bool(
            st.session_state.get("discovery_supplier_available_qogita")
        )
        st.session_state[f"discovery_supplier_{supplier}"] = value and available


def _sync_all_discovery_suppliers():
    selectable = [
        supplier for supplier in DISCOVERY_OPERATIONAL_SUPPLIERS
        if supplier != "qogita" or st.session_state.get("discovery_supplier_available_qogita")
    ]
    st.session_state["discovery_supplier_all"] = all(
        st.session_state.get(f"discovery_supplier_{supplier}", False)
        for supplier in selectable
    )


@st.cache_data(ttl=60, show_spinner=False)
def discovery_supplier_catalog_status():
    store = SupplierCatalogStore()
    return {
        supplier: (
            store.serving_generation_metadata(supplier) if supplier == "qogita"
            else store.active_generation_metadata(supplier)
        )
        for supplier in SUPPORTED_SUPPLIERS
    }


@st.cache_data(ttl=60, show_spinner=False)
def discovery_supplier_universe(selected_suppliers):
    if not selected_suppliers:
        return {"total": 0, "eligible": 0}
    return SupplierCatalogStore().active_identifier_universe(selected_suppliers)


@st.cache_data(ttl=60, show_spinner=False)
def discovery_rotation_diagnostics(selected_suppliers):
    """Load rotation diagnostics only after the user opens advanced controls."""
    if not selected_suppliers:
        return {"total": 0, "eligible": 0}
    identifiers = SupplierCatalogStore().active_identifiers(selected_suppliers)
    eligible_identifiers = {
        value for value in identifiers if canonical_gtin14(value) is not None
    }
    result = {"total": len(identifiers), "eligible": len(eligible_identifiers)}
    rotation = DiscoveryRotationStore().status(
        selected_suppliers, active_identifiers=eligible_identifiers,
    )
    return {**result, **rotation}


@st.cache_data(ttl=60, show_spinner=False)
def discovery_amazon_plan_preview(selected_suppliers):
    if not selected_suppliers:
        return {
            "requested_universe_count": 0, "cache_reuse_count": 0,
            "refresh_count": 0, "new_lookup_count": 0,
        }
    identifiers = SupplierCatalogStore().active_identifiers(selected_suppliers)
    eligible = [value for value in identifiers if canonical_gtin14(value) is not None]
    return DiscoveryAmazonCache(DiscoveryIncrementalStore()).preview_counts(
        eligible, AmazonFreshnessPolicy.from_environment(),
    )


def new_discovery_search():
    st.session_state["ui_state"] = "discovery"
    st.session_state.pop("discovery_result", None)
    st.session_state.pop("discovery_error", None)


def _open_discovery_job(job_id):
    st.session_state["discovery_job_id"] = job_id
    st.session_state["ui_state"] = "discovery_running"


def _start_discovery_worker(state):
    registry = DiscoveryJobRegistry()
    registry.register_checkpoint(state)
    pid = registry.launch(state["job_id"])
    st.session_state["discovery_job_id"] = state["job_id"]
    st.session_state["discovery_status"] = "running"
    st.session_state["ui_state"] = "discovery_running"
    return pid


def _load_discovery_result(job_id, runtime=None):
    state = DiscoveryCheckpointStore().load(job_id)
    runtime = runtime or DiscoveryJobRegistry().get(job_id) or {}
    operational = dict(state.get("operational_export") or {})
    technical = dict(state.get("technical_export") or {})
    output_path = operational.get("path") or runtime.get("export_path")
    technical_path = technical.get("path")

    def workbook_bytes(path):
        if path and os.path.isfile(path):
            with open(path, "rb") as output:
                return output.read()
        return None

    output_bytes = workbook_bytes(output_path)
    technical_output_bytes = (
        workbook_bytes(technical_path)
        if technical_path and os.path.realpath(technical_path) != os.path.realpath(output_path or "")
        else None
    )
    st.session_state["discovery_result"] = {
        "state": state,
        "output_bytes": output_bytes,
        "output_file_name": (
            operational.get("file_name") or os.path.basename(output_path or "")
            or f"{job_id}.xlsx"
        ),
        "technical_output_bytes": technical_output_bytes,
        "technical_output_file_name": (
            technical.get("file_name") or os.path.basename(technical_path or "")
        ),
    }
    st.session_state["discovery_status"] = state.get("status") or runtime.get("status")
    st.session_state["ui_state"] = "discovery_result"


def discovery_notification_status(job_id, database_path=None):
    if job_id:
        row = NotificationOutbox(
            database_path or DiscoveryJobRegistry().path
        ).get(job_id)
        if row:
            labels = {
                "sent": "Email: inviata",
                "not_configured": "Email: non configurata",
                "failed": "Email: invio fallito",
                "pending": "Email: in preparazione",
                "sending": "Email: invio in corso",
            }
            return labels.get(row.get("status"), "Email: stato non disponibile")
    config = EmailConfig.from_runtime()
    return "Email: attiva" if config.configured else "Email: non configurata"


def _discovery_compact_state(job_id):
    root = Path(os.environ.get("DISCOVERY_CHECKPOINT_ROOT", "data/discovery_jobs"))
    path = root / f"{job_id}.state.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _discovery_local_time(value):
    if not value:
        return "—"
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return "—"
    return parsed.astimezone(ZoneInfo("Europe/Rome")).strftime("%d/%m/%Y %H:%M")


def _discovery_completion_counts(runtime):
    state = _discovery_compact_state(runtime["job_id"])
    evaluated = int(
        state.get("selected_count") or state.get("sampled_identifier_count")
        or runtime.get("progress_total") or 0
    )
    opportunities = int(
        state.get("final_opportunity_count") or state.get("final_products") or 0
    )
    fee_target = int(state.get("fee_target_count") or 0)
    fee_valid = int(state.get("fee_valid_count") or 0)
    fee_unavailable = int(state.get("fee_unavailable_count") or 0)
    coverage = (100.0 * fee_valid / fee_target) if fee_target else None
    return {
        "state": state, "evaluated": evaluated, "opportunities": opportunities,
        "fee_target": fee_target, "fee_valid": fee_valid,
        "fee_unavailable": fee_unavailable, "fee_coverage": coverage,
    }


def _render_active_discovery(runtime, *, key_prefix):
    progress = discovery_phase_progress(runtime)
    st.subheader("DISCOVERY IN CORSO")
    st.markdown(f"**{progress['phase_label']}**")
    if progress["numeric"]:
        st.progress(progress["fraction"])
        st.caption(
            f"{progress['progress_label']}: {progress['current']:,} / "
            f"{progress['total']:,} · {progress['fraction'] * 100:.1f}%"
            .replace(",", ".")
        )
    else:
        st.caption("Fase in corso · avanzamento numerico non disponibile")
    st.caption(
        f"Ultimo aggiornamento: {_discovery_local_time(runtime.get('updated_at'))} · "
        f"{format_eta(discovery_phase_eta_seconds(runtime))}"
    )
    if st.button(
        "Apri avanzamento", key=f"{key_prefix}_open_progress", type="primary",
        use_container_width=True,
    ):
        _open_discovery_job(runtime["job_id"])
        st.rerun()


def _render_completed_discovery(runtime, *, key_prefix):
    counts = _discovery_completion_counts(runtime)
    st.subheader("DISCOVERY COMPLETATA")
    st.caption(
        f"{counts['opportunities']:,} opportunità · "
        f"{counts['evaluated']:,} prodotti valutati · "
        f"completata il {_discovery_local_time(runtime.get('completed_at'))}"
        .replace(",", ".")
    )
    if counts["fee_coverage"] is not None:
        st.caption(
            f"Copertura Fee {counts['fee_coverage']:.2f}% · "
            f"{counts['fee_unavailable']:,} non disponibili".replace(",", ".")
        )
    columns = st.columns(2)
    if columns[0].button(
        "Visualizza risultati / Scarica Excel",
        key=f"{key_prefix}_open_results", type="primary",
        use_container_width=True,
    ):
        _load_discovery_result(runtime["job_id"], runtime)
        st.rerun()
    if columns[1].button(
        "Nuova ricerca", key=f"{key_prefix}_new_search", use_container_width=True,
    ):
        new_discovery_search()
        st.rerun()


@st.fragment(run_every=2)
def _render_discovery_runtime(job_id):
    registry = DiscoveryJobRegistry()
    runtime = registry.get(job_id) if job_id else registry.latest()
    if not runtime:
        ui_alert("Stato Discovery non disponibile.", "warning")
        return
    if runtime["status"] in {
        "launching", "running", "computed", "export_pending",
        "export_running", "notification_pending",
    }:
        progress = discovery_phase_progress(runtime)
        st.subheader(progress["phase_label"])
        st.caption(format_phase_steps(runtime))
        if progress["numeric"]:
            st.progress(progress["fraction"])
            st.caption(
                f"{progress['progress_label']}: {progress['current']:,} / "
                f"{progress['total']:,} · {progress['fraction'] * 100:.1f}%"
                .replace(",", ".")
            )
        else:
            st.info("Fase in corso. Il progresso numerico non è ancora disponibile.")
        st.caption(
            f"Ultimo aggiornamento: {_discovery_local_time(runtime.get('updated_at'))} · "
            f"{format_eta(discovery_phase_eta_seconds(runtime))}"
        )
        if runtime["status"] in {"computed", "export_pending", "export_running"}:
            st.info("Calcolo completato. Generazione Excel in corso con memoria protetta.")
        elif runtime["status"] == "notification_pending":
            st.info("Excel completato. Preparazione della notifica finale in corso.")
        else:
            st.info("La Discovery continua anche se torni alla Home o chiudi il browser.")
        return
    if runtime["status"] == "completed":
        _load_discovery_result(runtime["job_id"], runtime)
        st.rerun(scope="app")
    elif runtime["status"] in {"resource_paused", "export_resource_paused"}:
        state = DiscoveryCheckpointStore().load(runtime["job_id"])
        pause = state.get("resource_pause") or {}
        st.warning("Discovery sospesa per proteggere HomeServer")
        st.caption(
            f"Fase: {state.get('progress_phase') or state.get('phase') or '—'} · "
            f"motivo: {pause.get('reason') or 'pressione risorse'} · "
            "il job e il campione restano riprendibili."
        )
        if st.button("Riprendi Discovery", key="resume_resource_paused", type="primary"):
            if runtime["status"] == "export_resource_paused":
                registry.launch_finalizer(runtime["job_id"])
            else:
                _start_discovery_worker(state)
            st.rerun(scope="app")
    elif runtime.get("resumable"):
        state = DiscoveryCheckpointStore().load(runtime["job_id"])
        st.warning(
            f"Discovery interrotta alla fase {state.get('phase')}. "
            "Il checkpoint e lo stesso campione sono disponibili."
        )
        if st.button("Riprendi Discovery", key="resume_current_discovery", type="primary"):
            _start_discovery_worker(state)
            st.rerun(scope="app")
    else:
        _load_discovery_result(runtime["job_id"], runtime)
        st.rerun(scope="app")


if "ui_state" not in st.session_state:
    st.session_state["ui_state"] = (
        "single_result"
        if st.session_state.get("single_product_result")
        else "home"
    )

ui_state = st.session_state["ui_state"]

if ui_state == "home":
    discovery_registry = DiscoveryJobRegistry()
    active_discovery = discovery_registry.latest_active()
    latest_discovery = discovery_registry.latest()
    if active_discovery:
        with st.container(border=True):
            _render_active_discovery(active_discovery, key_prefix="home_active")
    elif latest_discovery and latest_discovery.get("status") == "completed":
        with st.container(border=True):
            _render_completed_discovery(latest_discovery, key_prefix="home_completed")
    elif latest_discovery and latest_discovery.get("resumable"):
        with st.container(border=True):
            st.subheader("DISCOVERY SOSPESA")
            st.caption(
                "Il job persistito può essere aperto e ripreso senza creare un nuovo campione."
            )
            if st.button(
                "Apri avanzamento", key="home_resumable_open", type="primary"
            ):
                _open_discovery_job(latest_discovery["job_id"])
                st.rerun()
    else:
        with st.container(border=True):
            st.subheader("Scopri opportunità")
            st.caption(
                "Trova automaticamente i prodotti più interessanti da acquistare "
                "e vendere su Amazon"
            )
            email_config = EmailConfig.from_runtime()
            st.caption(
                f"Notifiche email: {'attive' if email_config.configured else 'disattive'} · "
                f"Destinatario: {email_config.recipient}"
            )
            if st.button(
                "Apri Discovery", key="open_discovery", type="primary",
                use_container_width=True,
            ):
                st.session_state["ui_state"] = "discovery"
                st.rerun()
    with st.container(border=True):
        st.subheader("Ricerca singola")
        st.markdown(
            '<div class="gu-field-label">EAN prodotto</div>',
            unsafe_allow_html=True,
        )
        with st.container(
            key="ean_operation_row",
            horizontal=True,
            vertical_alignment="center",
            gap="small",
        ):
            ean = st.text_input(
                "EAN prodotto",
                key="ean_input",
                placeholder="Inserisci il codice EAN",
                label_visibility="collapsed",
                width="stretch",
            )
            analyze_ean = st.button("Analizza EAN", type="primary", width=168)
        if analyze_ean:
            if not ean:
                ui_alert("Inserisci prima un EAN.", "warning")
            else:
                st.session_state.pop("single_product_result", None)
                st.session_state["single_status"] = "pending"
                st.session_state["single_ean"] = ean.strip()
                st.session_state["ui_state"] = "single_result"
                st.rerun()

    st.markdown("<div style='height:.55rem'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        st.subheader("Analisi multipla")
        st.caption("Carica un file Excel con colonne EAN e COSTO")
        uploaded_file = st.file_uploader(
            "File Excel", type=["xlsx"], label_visibility="collapsed",
        )
        if uploaded_file:
            df_input = read_input_excel(uploaded_file)
            if "EAN" not in df_input.columns:
                ui_alert(
                    "Il file deve contenere una colonna chiamata EAN.", "error"
                )
            else:
                costo_col = next(
                    (
                        col for col in df_input.columns
                        if str(col).strip().lower() == "costo"
                    ),
                    None,
                )
                st.caption(f"{len(df_input)} prodotti pronti per l’analisi")
                if st.button(
                    "Analizza Excel", type="primary", use_container_width=True
                ):
                    st.session_state["batch_input"] = df_input
                    st.session_state["batch_cost_column"] = costo_col
                    st.session_state["batch_source_file"] = getattr(
                        uploaded_file, "name", "<uploaded file>"
                    )
                    st.session_state["batch_status"] = "ready"
                    st.session_state["ui_state"] = "batch_running"
                    st.rerun()

    recent_discoveries = discovery_registry.recent(5, status="completed")
    if recent_discoveries:
        st.markdown("<div style='height:.55rem'></div>", unsafe_allow_html=True)
        st.subheader("Discovery recenti")
        for recent in recent_discoveries:
            counts = _discovery_completion_counts(recent)
            with st.container(border=True):
                columns = st.columns([3, 1])
                supplier_names = ", ".join(
                    str(value).upper()
                    for value in recent.get("selected_suppliers") or []
                )
                columns[0].markdown(
                    f"**{_discovery_local_time(recent.get('completed_at'))}**  \n"
                    f"{counts['evaluated']:,} prodotti · {counts['opportunities']:,} "
                    f"opportunità · fornitori {supplier_names or '—'} · "
                    f"Excel {'disponibile' if recent.get('export_path') else 'non disponibile'}"
                    .replace(",", ".")
                )
                if columns[1].button(
                    "Apri", key=f"history_open_{recent['job_id']}",
                    use_container_width=True,
                ):
                    _load_discovery_result(recent["job_id"], recent)
                    st.rerun()

elif ui_state == "single_result":
    st.button(
        "← Torna alla ricerca",
        key="back_single_home",
        type="secondary",
        on_click=return_to_home,
    )
    if st.session_state.get("single_status") == "pending":
        ean = st.session_state.get("single_ean", "")
        with st.spinner("Confronto fornitori e analisi Amazon in corso…"):
            try:
                token_provider = RefreshingTokenProvider(get_access_token)

                def direct_catalog_batch(gtins, job_id, products=None):
                    items = search_catalog_by_gtins_batch(
                        gtins, token_provider,
                        marketplace_id=os.environ["MARKETPLACE_ID"], job_id=job_id,
                    )
                    return correlate_catalog_items(gtins, items, products)

                def direct_pricing_batch(asins, job_id):
                    entries = get_item_offers_batch(
                        asins, token_provider,
                        marketplace_id=os.environ["MARKETPLACE_ID"], job_id=job_id,
                    )
                    return parse_item_offers_batch(entries)

                state = run_direct_lookup(
                    ean, catalog_batch=direct_catalog_batch,
                    pricing_batch=direct_pricing_batch,
                    fees_batch=search_product_fees_batch,
                    token_provider=token_provider,
                )
                st.session_state["single_product_result"] = {"state": state}
                st.session_state["single_status"] = "found"
            except Exception as error:
                logger.exception(
                    "SINGLE PRODUCT ANALYSIS FAILED | "
                    "phase=PROCESSING PRODUCT ean=%s",
                    ean,
                )
                st.session_state["single_status"] = "error"
                st.session_state["single_message"] = f"Errore: {error}"
        st.rerun()

    single_status = st.session_state.get("single_status")
    if single_status in ("not_found", "error"):
        ui_alert(
            st.session_state.get("single_message", "Prodotto non disponibile."),
            "error",
        )
    elif st.session_state.get("single_product_result"):
        single_product_result = st.session_state["single_product_result"]
        state = normalize_discovery_state(single_product_result["state"])
        product = (state.get("candidates") or [{}])[0]
        scenarios = product.get("scenarios") or []
        listings = product.get("amazon_listings") or []
        combination = product.get("recommended_combination") or {}
        scenario_by_id = {
            row.get("scenario_id"): row for row in scenarios
        }
        recommended = scenario_by_id.get(combination.get("scenario_id"))
        recommended_listing = next((
            row for row in listings
            if row.get("asin") == combination.get("asin")
        ), listings[0] if listings else {})
        title = (
            recommended_listing.get("title") or product.get("amazon_title")
            or product.get("title") or "Prodotto supplier"
        )
        brand = (
            recommended_listing.get("brand") or product.get("amazon_brand")
            or product.get("brand") or "—"
        )
        asin = recommended_listing.get("asin") or product.get("asin")
        ui_alert("Confronto completato.")
        with st.container(border=True):
            identity, summary = st.columns([1.05, 2.25], vertical_alignment="top")
            with identity:
                image_url = recommended_listing.get("main_image") or product.get("image_url")
                if image_url:
                    st.image(image_url, width="stretch")
                st.markdown(
                    '<div class="gu-product-brand">'
                    f'{html.escape(str(display_value(brand)))}'
                    '</div><div class="gu-product-title">'
                    f'{html.escape(str(display_value(title)))}'
                    '</div><div class="gu-meta">'
                    f'EAN {html.escape(str(product.get("canonical_ean") or state.get("ean_requested") or "—"))}<br>'
                    f'ASIN {html.escape(str(asin or "—"))}'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with summary:
                st.subheader("Migliore opzione di acquisto")
                if recommended and combination:
                    columns = st.columns(4)
                    columns[0].metric("Fornitore", str(recommended.get("supplier") or "—").upper())
                    columns[1].metric("Scenario", recommended.get("scenario_label") or "—")
                    columns[2].metric("Costo", format_eur(recommended.get("cost_gross_unit_eur")))
                    columns[3].metric("Requisito", scenario_requirement_label(recommended))
                    columns = st.columns(4)
                    columns[0].metric("Disponibilità", scenario_stock_availability(recommended))
                    columns[1].metric("Prezzo Amazon", format_eur(combination.get("price_reference")))
                    columns[2].metric("Utile", format_eur(combination.get("profit")))
                    columns[3].metric("Margine", format_percent(combination.get("margin_percent")))
                elif scenarios:
                    ui_alert(
                        "Le offerte supplier sono disponibili; l’economia Amazon non è disponibile.",
                        "warning",
                    )
                else:
                    ui_alert(
                        "Nessuna offerta supplier-first disponibile nelle baseline attive.",
                        "warning",
                    )

        status_labels = []
        for supplier in ("abw", "umma", "qudo", "qogita"):
            supplier_status = (state.get("supplier_snapshot_set") or {}).get(supplier, {})
            status = supplier_status.get("availability_status") or "unavailable"
            count = supplier_status.get("scenario_count", 0)
            label = f"{supplier.upper()} ✓ · {count} scenari" if status == "available" else (
                "QOGITA — prodotto a catalogo, scenari non ancora verificati"
                if supplier == "qogita" and status == "catalog_present_scenarios_pending" else
                "QOGITA — bootstrap scenari in corso" if supplier == "qogita"
                else f"{supplier.upper()} — EAN assente"
            )
            status_labels.append(label)
        st.caption(" · ".join(status_labels))

        st.subheader("Confronto fornitori")
        if scenarios:
            st.dataframe(
                direct_scenario_rows(state), hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("Nessuno scenario di acquisto disponibile nelle baseline supplier-first.")

        st.subheader("Amazon")
        if listings:
            for listing in listings:
                with st.container(border=True):
                    metrics = st.columns(4)
                    metrics[0].metric("ASIN", listing.get("asin") or "—")
                    metrics[1].metric("BSR Beauty", display_value(listing.get("bsr_beauty")))
                    metrics[2].metric("Prezzo riferimento", format_eur(listing.get("reference_price")))
                    buy_box = (
                        listing.get("reference_price")
                        if listing.get("price_source") == "buy_box" else None
                    )
                    metrics[3].metric("Buy Box", format_eur(buy_box))
                    metrics = st.columns(4)
                    metrics[0].metric("Venditori FBA", display_value(listing.get("fba_sellers")))
                    metrics[1].metric("Venditori totali", display_value(listing.get("total_sellers")))
                    metrics[2].metric("Prezzo minimo FBA", format_eur(listing.get("min_fba_price")))
                    metrics[3].metric("Prezzo minimo FBM", format_eur(listing.get("min_fbm_price")))
                    listing_asin = listing.get("asin")
                    if listing_asin:
                        with st.container(horizontal=True, gap="small"):
                            st.link_button(
                                "Vedi offerte Amazon",
                                f"https://www.amazon.it/gp/offer-listing/{listing_asin}",
                                type="primary", use_container_width=True,
                            )
                            st.link_button(
                                "Apri scheda Amazon",
                                f"https://www.amazon.it/dp/{listing_asin}",
                                type="primary", use_container_width=True,
                            )
        else:
            st.info(
                "Nessuna pagina Amazon trovata. Le offerte supplier restano disponibili sopra."
            )

elif ui_state == "batch_running":
    with st.container(border=True):
        st.subheader("Analisi multipla")
        st.caption("Elaborazione prodotti Amazon in corso")
        progress_status = None
        progress_widget = None
        try:
            progress_status = st.empty()
            progress_widget = st.progress(0)
        except Exception:
            logger.exception(
                "PROGRESS INITIALIZATION FAILED | phase=START ANALYSIS; "
                "continuing without UI progress"
            )

        if st.session_state.get("batch_status") == "ready":
            st.session_state["batch_status"] = "processing"
            df_input = st.session_state["batch_input"]
            costo_col = st.session_state["batch_cost_column"]
            source_file = st.session_state["batch_source_file"]
            output_file = "glowup_scout_output.xlsx"
            started_at = time.monotonic()
            phase = "START ANALYSIS"
            logger.info(
                "START ANALYSIS | products=%s file=%s",
                len(df_input), source_file,
            )

            def update_batch_progress(value):
                if progress_widget is not None:
                    progress_widget.progress(value)
                completed = min(
                    len(df_input), int((value / 0.8) * len(df_input))
                )
                if progress_status is not None:
                    progress_status.caption(
                        f"{completed} di {len(df_input)} prodotti completati"
                    )

            try:
                if progress_status is not None:
                    progress_status.caption(
                        f"0 di {len(df_input)} prodotti completati"
                    )
                token = get_access_token()
                phase = "PROCESSING PRODUCTS"
                df_results = analyze_products(
                    df_input=df_input,
                    costo_col=costo_col,
                    token=token,
                    search_catalog=search_catalog,
                    search_pricing=search_pricing,
                    search_fees_batch=search_product_fees_batch,
                    safe_call=safe_call,
                    progress_callback=update_batch_progress,
                    source_file=source_file,
                )
                phase = "ANALYSIS COMPLETED"
                logger.info(
                    "ANALYSIS COMPLETED | products=%s file=%s",
                    len(df_results), source_file,
                )
                phase = "WRITING EXCEL"
                generated_file = write_results_excel(df_results, output_file)
                duration_seconds = time.monotonic() - started_at
                result_summary = summarize_results(df_results)
                with open(generated_file, "rb") as output:
                    output_bytes = output.read()
                st.session_state["batch_result"] = {
                    "summary": result_summary,
                    "duration_seconds": duration_seconds,
                    "output_bytes": output_bytes,
                }
                st.session_state["batch_status"] = "completed"
                st.session_state["ui_state"] = "batch_result"
                logger.info(
                    "READY FOR DOWNLOAD | file=%s duration_seconds=%.2f",
                    generated_file, duration_seconds,
                )
            except Exception:
                logger.exception(
                    "BATCH ANALYSIS FAILED | phase=%s file=%s output_file=%s",
                    phase, source_file, output_file,
                )
                st.session_state["batch_status"] = "error"
                st.session_state["batch_error"] = (
                    f"Errore durante la fase '{phase}'. "
                    "Consulta i log per i dettagli."
                )
                st.session_state["ui_state"] = "batch_result"
            st.rerun()

elif ui_state == "batch_result":
    st.button(
        "← Torna alla home",
        key="back_batch_home",
        type="secondary",
        on_click=return_to_home,
    )
    if st.session_state.get("batch_status") == "error":
        ui_alert(
            st.session_state.get("batch_error", "Analisi non completata."),
            "error",
        )
    elif st.session_state.get("batch_result"):
        batch_result = st.session_state["batch_result"]
        result_summary = batch_result["summary"]
        ui_alert("Analisi completata. Il file è pronto.")
        with st.container(border=True):
            summary_columns = st.columns(4)
            summary_columns[0].metric(
                "Prodotti analizzati", result_summary["total"]
            )
            summary_columns[1].metric(
                "Prodotti idonei", result_summary["eligible"]
            )
            summary_columns[2].metric(
                "Prodotti non idonei", result_summary["not_eligible"]
            )
            summary_columns[3].metric(
                "Durata", f"{batch_result['duration_seconds']:.1f} s"
            )
            st.download_button(
                label="Scarica risultato Excel",
                data=batch_result["output_bytes"],
                file_name="glowup_scout_output.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
                on_click="ignore",
                type="primary",
                use_container_width=True,
            )

elif ui_state == "discovery":
    st.button(
        "← Torna alla home",
        key="back_discovery_home",
        type="secondary",
        on_click=return_to_home,
    )
    with st.container(border=True):
        st.subheader("Scopri opportunità")
        st.caption(
            "Trova automaticamente i prodotti più interessanti da acquistare "
            "e vendere su Amazon"
        )
        email_config = EmailConfig.from_runtime()
        st.caption(
            f"Notifiche email: {'attive' if email_config.configured else 'disattive'} · "
            f"Destinatario: {email_config.recipient}"
        )
        active_discovery = DiscoveryJobRegistry().latest_active()
        if active_discovery:
            current = int(active_discovery.get("progress_current") or 0)
            total = int(active_discovery.get("progress_total") or 0)
            ui_alert(
                f"Discovery in corso · {current:,} / {total:,} · "
                f"fase {str(active_discovery.get('phase') or 'preparazione').replace('_', ' ')}"
                .replace(",", "."),
                "warning",
            )
            if st.button(
                "Apri avanzamento", key="open_running_discovery",
                type="primary", use_container_width=True,
            ):
                _open_discovery_job(active_discovery["job_id"])
                st.rerun()
        st.markdown("**Fornitori**")
        catalog_statuses = discovery_supplier_catalog_status()
        qogita_available = bool(catalog_statuses.get("qogita"))
        st.session_state["discovery_supplier_available_qogita"] = qogita_available
        for supplier in SUPPORTED_SUPPLIERS:
            st.session_state.setdefault(
                f"discovery_supplier_{supplier}",
                supplier in DISCOVERY_OPERATIONAL_SUPPLIERS
                and (supplier != "qogita" or qogita_available),
            )
        if not qogita_available:
            st.session_state["discovery_supplier_qogita"] = False
        st.session_state.setdefault("discovery_supplier_all", True)
        if qogita_available and st.session_state.get("discovery_supplier_all"):
            st.session_state["discovery_supplier_qogita"] = True
        supplier_columns = st.columns(5)
        supplier_columns[0].checkbox(
            "Tutti", key="discovery_supplier_all",
            on_change=_toggle_all_discovery_suppliers,
        )
        supplier_labels = {
            "qogita": "Qogita", "umma": "UMMA", "abw": "ABW", "qudo": "Qudo",
        }
        for column, supplier in zip(supplier_columns[1:], SUPPORTED_SUPPLIERS):
            column.checkbox(
                supplier_labels[supplier], key=f"discovery_supplier_{supplier}",
                on_change=_sync_all_discovery_suppliers,
                disabled=supplier == "qogita" and not qogita_available,
                help=(
                    "Bootstrap scenari in corso: Qogita diventa selezionabile dopo "
                    "il primo snapshot serving verificato."
                    if supplier == "qogita" and not qogita_available else None
                ),
            )
        selected_suppliers = [
            supplier for supplier in DISCOVERY_OPERATIONAL_SUPPLIERS
            if st.session_state.get(f"discovery_supplier_{supplier}")
        ]
        coverage_lines = []
        for supplier in SUPPORTED_SUPPLIERS:
            status = catalog_statuses.get(supplier)
            if not status:
                coverage_lines.append(
                    f"**{supplier_labels[supplier]}** · baseline non disponibile"
                )
                continue
            if supplier == "qogita" and status.get("serving_snapshot"):
                coverage_lines.append(
                    f"**Qogita** · Catalogo {int(status.get('product_catalog_count') or 0):,} prodotti · "
                    f"Scenari verificati {int(status.get('enriched_product_count') or 0):,} / "
                    f"{int(status.get('product_catalog_count') or 0):,} · "
                    f"Copertura {float(status.get('coverage_percent') or 0):.2f}% · "
                    f"Bootstrap {status.get('duty_state') or 'in corso'} · "
                    f"ultimo snapshot {status.get('created_at') or '—'} · "
                    f"prossima transizione {status.get('current_window_deadline') or status.get('rest_until') or '—'}"
                )
            else:
                coverage_lines.append(
                    f"**{supplier_labels[supplier]}** · "
                    f"{int(status.get('product_count') or 0):,} prodotti · "
                    f"{int(status.get('scenario_count') or 0):,} scenari · "
                    f"catalogo {str(status.get('product_catalog_coverage_type') or status.get('coverage_type') or 'parziale').replace('_', ' ')} · "
                    f"scenari {str(status.get('scenario_enrichment_status') or 'none')}"
                )
        st.markdown("**Filtri**")
        defaults = default_filters()
        first = st.columns(3)
        bsr_min = first[0].number_input(
            "BSR minimo", min_value=0, value=defaults["bsr_min"], step=100
        )
        bsr_max = first[1].number_input(
            "BSR massimo", min_value=1, value=defaults["bsr_max"], step=100
        )
        max_fba = first[2].number_input(
            "Venditori FBA massimi", min_value=0, value=defaults["max_fba_sellers"], step=1
        )
        second = st.columns(2)
        max_total = second[0].number_input(
            "Venditori totali massimi", min_value=0, value=defaults["max_total_sellers"], step=1
        )
        minimum_margin = second[1].number_input(
            "Margine minimo %", min_value=0, max_value=100,
            value=defaults["minimum_margin"], step=1,
        )
        filters = {
            "bsr_min": bsr_min,
            "bsr_max": bsr_max,
            "max_fba_sellers": max_fba,
            "max_total_sellers": max_total,
            "minimum_margin": minimum_margin,
            "minimum_qogita_stock": defaults["minimum_qogita_stock"],
        }
        category_filter = default_qogita_category_filter()
        if "qogita" in selected_suppliers:
            st.markdown("**Categorie Qogita**")
            all_categories = st.checkbox(
                "Tutte le categorie", value=True,
                key="discovery_qogita_all_categories",
                help="Include anche categorie Amazon nuove o non ancora classificate.",
            )
            if not all_categories:
                only_beauty = st.checkbox(
                    "Solo Beauty", value=False,
                    key="discovery_qogita_only_beauty",
                    help="Si applica esclusivamente agli scenari Qogita.",
                )
                parent_options = [
                    node_id for node_id in QOGITA_CATEGORY_TREE
                    if not only_beauty or node_id in BEAUTY_PARENT_IDS
                ]
                selected_parent_ids = st.multiselect(
                    "Categorie principali",
                    options=parent_options,
                    default=parent_options,
                    format_func=lambda value: QOGITA_CATEGORY_TREE[value]["label"],
                    key=f"discovery_qogita_category_parents_{int(only_beauty)}",
                )
                include_unknown = st.checkbox(
                    "Includi prodotti non classificati", value=True,
                    key="discovery_qogita_include_unknown",
                )
                child_overrides = {}
                for parent_id in selected_parent_ids:
                    children = QOGITA_CATEGORY_TREE[parent_id]["children"]
                    if not children:
                        continue
                    with st.expander(
                        f"Dettaglio · {QOGITA_CATEGORY_TREE[parent_id]['label']}",
                        expanded=False,
                    ):
                        excluded_ids = st.multiselect(
                            "Escludi sottocategorie o tipologie",
                            options=list(children), default=[],
                            format_func=lambda value, values=children: values[value],
                            key=f"discovery_qogita_excluded_{parent_id}",
                        )
                    if excluded_ids:
                        child_overrides[parent_id] = {"excluded_ids": excluded_ids}
                category_filter.update({
                    "qogita_category_filter_enabled": True,
                    "qogita_category_selected_parent_ids": selected_parent_ids,
                    "qogita_category_child_overrides": child_overrides,
                    "qogita_category_include_unknown": include_unknown,
                    "qogita_category_only_beauty": only_beauty,
                })
                selected_labels = [
                    QOGITA_CATEGORY_TREE[value]["label"]
                    for value in selected_parent_ids
                ]
                selection_summary = (
                    ", ".join(selected_labels)
                    if 0 < len(selected_labels) <= 4
                    else f"{len(selected_labels)} categorie selezionate"
                    if selected_labels else "nessuna categoria nota"
                )
                st.caption(
                    "Qogita: " + selection_summary
                    + (" · non classificati inclusi" if include_unknown else "")
                )
        try:
            universe = discovery_supplier_universe(tuple(selected_suppliers))
        except Exception:
            universe = {"total": 0, "eligible": 0}
        eligible = int(universe.get("eligible") or 0)
        st.metric("Prodotti da valutare", f"{eligible:,}".replace(",", "."))
        st.caption(
            "Scout riutilizzerà automaticamente i dati Amazon recenti e "
            "aggiornerà solo quelli necessari."
        )
        validation_error = discovery_filter_error(filters, selected_suppliers)
        if not validation_error and eligible == 0:
            validation_error = "Nessun prodotto idoneo nelle baseline selezionate."
        if validation_error:
            ui_alert(validation_error, "warning")
        confirmation_signature = (
            tuple(selected_suppliers), tuple(sorted(filters.items())), eligible,
            json.dumps(category_filter, sort_keys=True, separators=(",", ":")),
        )
        pending_confirmation = st.session_state.get("discovery_full_confirmation")
        if pending_confirmation != confirmation_signature:
            st.session_state.pop("discovery_full_confirmation", None)
            pending_confirmation = None
        if st.button(
            "Trova opportunità", key="start_discovery", type="primary",
            use_container_width=True,
            disabled=bool(validation_error) or bool(active_discovery),
        ):
            st.session_state["discovery_full_confirmation"] = confirmation_signature
            st.rerun()
        if pending_confirmation == confirmation_signature:
            st.warning(
                (
                    f"Stai per valutare {eligible:,} prodotti. Scout riutilizzerà "
                    "automaticamente la cache disponibile e aggiornerà solo i dati necessari."
                ).replace(",", ".")
            )
            confirmation_columns = st.columns(2)
            if confirmation_columns[0].button(
                "Annulla", key="discovery_cancel_full_catalog",
            ):
                st.session_state.pop("discovery_full_confirmation", None)
                st.rerun()
            if confirmation_columns[1].button(
                "Avvia Discovery", key="discovery_start_full_catalog", type="primary",
            ):
                st.session_state["discovery_filters"] = filters
                st.session_state["discovery_selected_suppliers"] = selected_suppliers
                store = DiscoveryCheckpointStore()
                state = store.create(filters)
                state["selected_suppliers"] = selected_suppliers
                state["run_budget"] = "all"
                state["discovery_planner_version"] = "automatic_amazon_freshness_v1"
                state["freshness_policy_version"] = (
                    AmazonFreshnessPolicy.from_environment().version
                )
                state.update(category_filter)
                state["progress_phase"] = "initialized"
                state["progress_current"] = 0
                state["progress_total"] = eligible
                store.save(state)
                st.session_state.pop("discovery_full_confirmation", None)
                _start_discovery_worker(state)
                st.rerun()

        technical_details = st.toggle(
            "Dettagli tecnici", value=False, key="discovery_technical_details",
        )
        if technical_details:
            st.caption("  \n".join(coverage_lines).replace(",", "."))
            try:
                rotation_details = discovery_rotation_diagnostics(
                    tuple(selected_suppliers)
                )
            except Exception:
                logger.exception("DISCOVERY ROTATION DIAGNOSTICS FAILED")
                st.warning("Diagnostica tecnica temporaneamente non disponibile.")
                rotation_details = {}
            if rotation_details:
                remaining = int(
                    rotation_details.get("rotation_remaining_count") or 0
                )
                global_analyzed = int(
                    rotation_details.get("rotation_global_analyzed_count") or 0
                )
                new_identifiers = int(
                    rotation_details.get("rotation_new_identifier_count") or 0
                )
                st.caption(
                    f"Scope {rotation_details.get('rotation_scope') or '—'} · "
                    f"ciclo {int(rotation_details.get('rotation_cycle_id') or 1)} · "
                    f"universo {int(rotation_details.get('total') or 0):,} · analizzati "
                    f"{int(rotation_details.get('rotation_analyzed_count') or 0):,} · "
                    f"rimanenti {remaining:,} · storico globale {global_analyzed:,} · "
                    f"nuovi {new_identifiers:,} · policy {POLICY_VERSION}"
                    .replace(",", ".")
                )
                added_suppliers = rotation_details.get("rotation_added_suppliers") or []
                if rotation_details.get("rotation_previous_scope") and added_suppliers:
                    added_labels = " · ".join(
                        supplier_labels.get(value, value.upper())
                        for value in added_suppliers
                    )
                    ui_alert(
                        (
                            f"Nuovo insieme fornitori: {added_labels} è stato aggiunto. "
                            f"Lo scope precedente conserva "
                            f"{int(rotation_details.get('rotation_previous_analyzed_count') or 0):,} "
                            "EAN analizzati; la storia Amazon globale non è stata cancellata."
                        ).replace(",", "."),
                        "info",
                    )
                can_start_new_cycle = bool(
                    rotation_details.get("rotation_scope_initialized")
                )
                if st.button(
                    "Nuovo ciclo Discovery", key="discovery_new_cycle",
                    disabled=bool(active_discovery) or not can_start_new_cycle,
                ):
                    st.session_state[
                        "discovery_new_cycle_confirmation_scope"
                    ] = rotation_details.get("rotation_scope")
                    st.rerun()
                confirmation_scope = st.session_state.get(
                    "discovery_new_cycle_confirmation_scope"
                )
                if confirmation_scope == rotation_details.get("rotation_scope"):
                    st.warning(
                        "Conferma il nuovo ciclo tecnico. Lo storico globale sarà preservato."
                    )
                    confirm_columns = st.columns(2)
                    if confirm_columns[0].button(
                        "Conferma nuovo ciclo", key="discovery_confirm_new_cycle",
                        type="primary",
                    ):
                        DiscoveryRotationStore().start_new_cycle(
                            selected_suppliers, confirmed=True
                        )
                        discovery_rotation_diagnostics.clear()
                        st.session_state.pop(
                            "discovery_new_cycle_confirmation_scope", None
                        )
                        st.rerun()
                    if confirm_columns[1].button(
                        "Annulla", key="discovery_cancel_new_cycle"
                    ):
                        st.session_state.pop(
                            "discovery_new_cycle_confirmation_scope", None
                        )
                        st.rerun()
            incomplete = DiscoveryCheckpointStore().latest_incomplete()
            if incomplete and not active_discovery and st.button(
                "Riprendi ultima Discovery incompleta",
                key="resume_discovery",
                type="secondary",
                use_container_width=True,
            ):
                st.session_state["discovery_filters"] = incomplete["filters"]
                st.session_state["discovery_selected_suppliers"] = incomplete.get(
                    "selected_suppliers"
                ) or ["qogita"]
                st.session_state["discovery_run_budget"] = incomplete.get(
                    "run_budget", "all"
                )
                st.session_state["discovery_job_id"] = incomplete["job_id"]
                _start_discovery_worker(incomplete)
                st.rerun()

elif ui_state == "discovery_running":
    st.button(
        "← Torna alla home", key="back_running_discovery_home",
        type="secondary", on_click=return_to_home,
    )
    with st.container(border=True):
        st.subheader("Scopri opportunità")
        job_id = st.session_state.get("discovery_job_id")
        _render_discovery_runtime(job_id)

elif ui_state == "discovery_result":
    st.button(
        "← Torna alla home", key="back_discovery_result_home",
        type="secondary", on_click=return_to_home,
    )
    if st.session_state.get("discovery_status") in {
        "error", "qogita_refresh_failed", "supplier_preparation_failed",
    }:
        ui_alert(st.session_state.get("discovery_error"), "error")
        failed_result = st.session_state.get("discovery_result") or {}
        failed_state = failed_result.get("state") or {}
        snapshots = failed_state.get("qogita_snapshot_before") or {}
        if snapshots:
            st.caption(
                "Ultimo aggiornamento Qogita: "
                + max(str(value) for value in snapshots.values())
            )
    elif st.session_state.get("discovery_result"):
        discovery_result = st.session_state["discovery_result"]
        state = normalize_discovery_state(discovery_result["state"])
        if state.get("checkpoint_compatibility") == "legacy_incompatible":
            ui_alert(LEGACY_CHECKPOINT_MESSAGE, "warning")
            st.stop()
        result_count = len(state.get("results") or [])
        evaluated_count = int(
            state.get("selected_count") or state.get("sampled_identifier_count") or 0
        )
        fee_target_count = int(state.get("fee_target_count") or 0)
        fee_valid_count = int(state.get("fee_valid_count") or 0)
        fee_unavailable_count = int(state.get("fee_unavailable_count") or 0)
        fee_coverage_percent = (
            100.0 * fee_valid_count / fee_target_count if fee_target_count else 0.0
        )
        st.subheader("DISCOVERY COMPLETATA")
        completion_columns = st.columns(3)
        completion_columns[0].metric("Opportunità trovate", result_count)
        completion_columns[1].metric("Prodotti valutati", f"{evaluated_count:,}".replace(",", "."))
        completion_columns[2].metric("Copertura Fee", f"{fee_coverage_percent:.2f}%")
        st.caption(
            f"{fee_unavailable_count:,} Fee non disponibili · completata il "
            f"{_discovery_local_time(state.get('completed_at'))}".replace(",", ".")
        )
        st.caption(discovery_notification_status(state.get("job_id")))
        ui_alert(
            f"{result_count} opportunità trovate" if result_count
            else "Nessuna opportunità con i filtri utilizzati.",
            "success" if result_count else "info",
        )
        action_columns = st.columns(3)
        if discovery_result.get("output_bytes") is not None:
            action_columns[0].download_button(
                "SCARICA EXCEL",
                data=discovery_result["output_bytes"],
                file_name=discovery_result.get("output_file_name")
                or f"glowup_scout_discovery_{state['job_id']}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary", use_container_width=True, on_click="ignore",
            )
        if discovery_result.get("technical_output_bytes") is not None:
            action_columns[1].download_button(
                "Scarica export tecnico completo",
                data=discovery_result["technical_output_bytes"],
                file_name=discovery_result.get("technical_output_file_name")
                or f"glowup_scout_discovery_{state['job_id']}.technical.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="secondary", use_container_width=True, on_click="ignore",
            )
        if action_columns[2].button(
            "← Nuova ricerca", key="new_discovery_search",
            type="secondary", on_click=new_discovery_search,
            use_container_width=True,
        ):
            pass
        snapshot_set = state.get("supplier_snapshot_set") or {}
        warnings = state.get("supplier_warnings") or []
        for warning in warnings:
            ui_alert(warning, "warning")
        if snapshot_set:
            supplier_summary = []
            for supplier in state.get("selected_suppliers") or []:
                supplier_state = snapshot_set.get(supplier) or {}
                if supplier_state.get("availability_status") == "available":
                    supplier_summary.append(
                        f"{supplier.upper()} · {supplier_state.get('products_count', 0)} prodotti · "
                        f"aggiornato {supplier_state.get('snapshot_at') or 'n/d'} · "
                        f"prossimo refresh {supplier_state.get('next_refresh_at') or 'n/d'}"
                    )
                else:
                    supplier_summary.append(f"{supplier.upper()} · non disponibile")
            st.caption("  ·  ".join(supplier_summary))
        snapshots = state.get("qogita_snapshot_after") or {}
        refresh_status = state.get("qogita_refresh_status")
        refresh_label = (
            "Cache recente" if refresh_status == "cache_fresh"
            else "Catalogo aggiornato"
        )
        if snapshots:
            st.caption(
                f"Qogita · {refresh_label} · "
                f"{max(str(value) for value in snapshots.values())} · "
                f"refresh {float(state.get('qogita_refresh_duration_seconds') or 0):.1f} s"
            )
        with st.container(border=True):
            st.subheader("Riepilogo")
            funnel = discovery_funnel_view(state)
            st.caption(
                f"Universo supplier: {int(state.get('total_supplier_ean_universe') or 0):,} EAN · "
                f"prodotti valutati: {int(state.get('sampled_identifier_count') or len(state.get('candidates') or [])):,}"
                .replace(",", ".")
            )
            summary_columns = st.columns(4)
            summary_columns[0].metric(
                "Prodotti analizzati", funnel["suppliers"]["supplier_products_total"]
            )
            summary_columns[1].metric(
                "Pagine Amazon valutate", funnel["listings"]["amazon_listings_found"]
            )
            summary_columns[2].metric(
                "Scenari acquisto", funnel["suppliers"]["supplier_scenarios_total"]
            )
            summary_columns[3].metric("Opportunità finali", result_count)
            fee_target = int(
                state.get("fee_target_count")
                or (state.get("funnel") or {}).get("fee_target_count")
                or 0
            )
            fee_unavailable = int(
                state.get("fee_unavailable_count")
                or (state.get("funnel") or {}).get("fee_unavailable_count")
                or 0
            )
            if fee_target and fee_unavailable:
                fee_valid = int(
                    state.get("fee_valid_count")
                    or (state.get("funnel") or {}).get("fee_valid_count")
                    or 0
                )
                st.warning(
                    f"{fee_valid}/{fee_target} Fee Amazon disponibili · "
                    f"{fee_unavailable} escluse dal ranking economico"
                )
            st.caption("Prodotti")
            product_labels = [
                ("Prodotti supplier", "supplier_products_total"),
                ("Trovati Amazon", "amazon_found"),
                ("Beauty valida", "beauty_valid"),
                ("BSR nel range", "bsr_passed"),
                ("Concorrenza valida", "competition_passed"),
                ("Fee valide", "fee_valid"),
                ("Opportunità", "final_opportunities"),
            ]
            for start in range(0, len(product_labels), 3):
                columns = st.columns(3)
                for column, (label, key) in zip(columns, product_labels[start:start + 3]):
                    value = (
                        funnel["suppliers"][key]
                        if key == "supplier_products_total" else funnel["products"][key]
                    )
                    column.metric(label, value)
            st.caption("Scenari acquisto")
            scenario_labels = [
                ("Disponibili", "supplier_scenarios_total"),
                ("Valutati", "scenarios_evaluated"),
                ("Margine minimo", "scenarios_margin_passed"),
                ("Sotto soglia", "scenarios_margin_below_threshold"),
            ]
            columns = st.columns(4)
            for column, (label, key) in zip(columns, scenario_labels):
                value = (
                    funnel["suppliers"][key]
                    if key == "supplier_scenarios_total" else funnel["scenarios"][key]
                )
                column.metric(label, value)
            if any(funnel["listings"].values()):
                st.caption("Pagine Amazon")
                listing_labels = [
                    ("Trovate", "amazon_listings_found"),
                    ("Compatibili", "compatible_listings"),
                    ("Beauty", "beauty_listings"),
                    ("BSR nel range", "bsr_passed_listings"),
                    ("Concorrenza valida", "competition_passed_listings"),
                    ("Fee valide", "fee_valid_listings"),
                ]
                columns = st.columns(3)
                for index, (label, key) in enumerate(listing_labels):
                    columns[index % 3].metric(label, funnel["listings"][key])
                st.caption(
                    "Compatibili = pagine Amazon riconosciute come compatibili "
                    "con il prodotto supplier. Non indica opportunità finali."
                )
                st.caption("Combinazioni")
                columns = st.columns(3)
                for column, (label, key) in zip(columns, (
                    ("Valutate", "combinations_evaluated"),
                    ("Margine minimo", "combinations_margin_passed"),
                    ("Sotto soglia", "combinations_margin_below_threshold"),
                )):
                    column.metric(label, funnel["combinations"][key])
            st.caption(
                "I prodotti esclusi dai filtri restano disponibili nell'Excel "
                "per la verifica manuale."
            )

        for row in state["results"][:50]:
            observation = row.get("amazon_observation") or row
            recommended = recommended_scenario(row)
            combination = recommended_combination(row)
            if recommended is None or (
                row.get("opportunity_combinations") and combination is None
            ):
                logger.error(
                    "DISCOVERY RESULT INCOMPATIBLE | job_id=%s gtin=%s "
                    "reason=missing_valid_recommended_scenario",
                    state.get("job_id"), row.get("gtin"),
                )
                ui_alert(LEGACY_CHECKPOINT_MESSAGE, "warning")
                continue
            with st.container(border=True):
                identity, metrics = st.columns([1.35, 2.65], vertical_alignment="top")
                with identity:
                    image_url = (
                        row.get("image_url")
                        or next((
                            listing.get("main_image")
                            for listing in row.get("amazon_listings") or []
                            if listing.get("asin") == row.get("asin")
                            and listing.get("main_image")
                        ), None)
                    )
                    if image_url:
                        st.image(image_url, width=120)
                    st.markdown(
                        f"**{html.escape(str(row.get('amazon_brand') or row.get('brand') or '—'))}**  \n"
                        f"{html.escape(str(row.get('amazon_title') or row.get('title') or '—'))}  \n"
                        f"GTIN {html.escape(str(row.get('gtin')))} · "
                        f"{html.escape(str(recommended.get('supplier') or '—').upper())}  \n"
                        f"{html.escape(str(recommended.get('scenario_label') or '—'))} · "
                        f"{len(row.get('scenarios') or [])} scenari acquisto · "
                        f"{len(row.get('amazon_listings') or [observation])} pagine Amazon"
                    )
                with metrics:
                    columns = st.columns(4)
                    columns[0].metric("Costo ivato", f"€ {recommended['cost_gross_unit_eur']:.2f}")
                    columns[1].metric(
                        "Requisito", scenario_requirement_label(recommended)
                    )
                    columns[2].metric("BSR Beauty", observation["bsr_beauty"])
                    columns[3].metric("Margine", f"{recommended['margin_percent']:.2f}%")
                    columns = st.columns(4)
                    columns[0].metric("Prezzo riferimento", f"€ {observation['reference_price']:.2f}")
                    columns[1].metric("Venditori FBA", observation["fba_sellers"])
                    columns[2].metric("Venditori totali", observation["total_sellers"])
                    columns[3].metric("Score", f"{recommended['score']} · {recommended['opportunity']}")
                    if combination:
                        st.caption(f"ASIN consigliato: {combination.get('asin')}")
                    st.link_button(
                        "Vedi offerte Amazon", row["amazon_offers_url"],
                        type="primary", use_container_width=True,
                    )
                    detail_key = f"show_scenarios_{row.get('product_key') or row.get('gtin')}"
                    if st.button(
                        "Confronta scenari", key=f"toggle_{detail_key}",
                        type="secondary", use_container_width=True,
                    ):
                        st.session_state[detail_key] = not st.session_state.get(detail_key, False)
                    if st.session_state.get(detail_key, False):
                        listing_rows = []
                        for listing in row.get("amazon_listings") or []:
                            listing_rows.append({
                                "ASIN": listing.get("asin"),
                                "Titolo": listing.get("title") or "—",
                                "BSR Beauty": listing.get("bsr_beauty"),
                                "Prezzo": listing.get("reference_price"),
                                "Min FBA": listing.get("min_fba_price"),
                                "Min FBM": listing.get("min_fbm_price"),
                                "Venditori FBA": listing.get("fba_sellers"),
                                "Venditori totali": listing.get("total_sellers"),
                                "Stato": listing.get("evaluation_status") or listing.get("compatibility_status"),
                            })
                        if listing_rows:
                            st.caption("Pagine Amazon")
                            st.dataframe(
                                listing_rows, hide_index=True,
                                use_container_width=True,
                            )
                        scenario_by_id = {
                            item.get("scenario_id"): item
                            for item in row.get("scenarios") or []
                        }
                        combination_rows = []
                        combination_values = row.get("opportunity_combinations") or []
                        if not combination_values:
                            combination_values = [{
                                "scenario_id": scenario.get("scenario_id"),
                                "asin": observation.get("asin"),
                                "price_reference": observation.get("reference_price"),
                                "margin_percent": scenario.get("margin_percent"),
                                "score": scenario.get("score"),
                                "opportunity": scenario.get("opportunity"),
                                "economics": scenario.get("economics") or {},
                            } for scenario in row.get("scenarios") or []]
                        for item in combination_values:
                            scenario = scenario_by_id.get(item.get("scenario_id"), {})
                            economics = item.get("economics") or {}
                            combination_rows.append({
                                "Fornitore": str(scenario.get("supplier") or "").upper(),
                                "Scenario": scenario.get("scenario_label") or "—",
                                "ASIN": item.get("asin"),
                                "Requisito": scenario_requirement_label(scenario),
                                "Costo netto": f"€ {scenario['cost_net_unit_eur']:.2f}",
                                "Costo ivato": f"€ {scenario['cost_gross_unit_eur']:.2f}",
                                "Stock": (
                                    str(scenario.get("stock"))
                                    if scenario.get("stock") is not None else "—"
                                ),
                                "Warehouse": scenario.get("warehouse") or "—",
                                "Disponibilità": (
                                    scenario.get("availability_text")
                                    or scenario.get("availability_status")
                                    or "—"
                                ),
                                "Lead time": scenario.get("lead_time") or "—",
                                "Prezzo Amazon": f"€ {item['price_reference']:.2f}",
                                "Margine": f"{item['margin_percent']:.2f}%",
                                "Prezzo 15%": f"€ {target_price(economics, 15):.2f}",
                                "Prezzo 20%": f"€ {target_price(economics, 20):.2f}",
                                "Prezzo 25%": f"€ {target_price(economics, 25):.2f}",
                                "Score": item["score"],
                                "Opportunità": item["opportunity"],
                                "Ruolo": (
                                    "Raccomandata" if combination and item.get("combination_id") == combination.get("combination_id") else ""
                                ),
                            })
                        if combination_rows:
                            st.caption("Combinazioni")
                            st.dataframe(
                                combination_rows, hide_index=True,
                                use_container_width=True,
                            )
        filters_used = state.get("filters") or {}
        st.caption(
            "Filtri utilizzati · "
            f"BSR {filters_used.get('bsr_min')}–{filters_used.get('bsr_max')} · "
            f"FBA max {filters_used.get('max_fba_sellers')} · "
            f"venditori max {filters_used.get('max_total_sellers')} · "
            f"margine min {filters_used.get('minimum_margin')}% · "
            f"fornitori {', '.join(state.get('selected_suppliers') or ['Qogita'])}"
        )
