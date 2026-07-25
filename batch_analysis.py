import logging
import os
import tempfile
import time

import pandas as pd


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ELIGIBLE_STATUS = "TROVATO CON OFFERTE"


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


def analyze_products(
    df_input,
    costo_col,
    token,
    search_catalog,
    search_pricing,
    safe_call,
    progress_callback=None,
    throttle_seconds=0.7,
    source_file=None,
):
    """Run the current batch analysis without depending on Streamlit."""
    results = []
    total_products = len(df_input)
    active_progress_callback = progress_callback

    logger.info(
        "PROCESSING PRODUCTS | products=%s file=%s",
        total_products,
        source_file or "<unknown>",
    )

    for position, (_, row) in enumerate(df_input.iterrows(), start=1):
        ean_value = str(row["EAN"]).strip()
        costo_value = row[costo_col] if costo_col else ""

        try:
            catalog = safe_call(search_catalog, ean_value, token)

            if catalog:
                pricing = safe_call(search_pricing, catalog["ASIN"], token)
                score, opportunity = opportunity_score(
                    catalog["BSR Beauty"],
                    pricing["Venditori totali"],
                )

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
                    "Score": score,
                    "Opportunità": opportunity,
                    "Decisione": decision_from_score(score),
                    "Link Amazon": f"https://www.amazon.it/dp/{catalog['ASIN']}",
                    "Link Offerte": f"https://www.amazon.it/gp/offer-listing/{catalog['ASIN']}",
                    "Stato": "TROVATO",
                    "Errore": "",
                })
            else:
                # Keep the existing result shape and post-processing behavior.
                results.append({
                    "EAN": ean_value,
                    "Costo": costo_value,
                    "Stato": "TROVATO",
                    "Errore": "",
                })

        except Exception as exc:
            logger.exception(
                "PRODUCT PROCESSING FAILED | phase=PROCESSING PRODUCTS "
                "file=%s ean=%s",
                source_file or "<unknown>",
                ean_value,
            )
            results.append({
                "EAN": ean_value,
                "Costo": costo_value,
                "Stato": "ERRORE API / LIMITE AMAZON",
                "Errore": str(exc),
            })

        if active_progress_callback is not None and total_products:
            try:
                active_progress_callback(position / total_products)
            except Exception:
                logger.exception(
                    "PROGRESS UPDATE FAILED | phase=PROCESSING PRODUCTS "
                    "file=%s; continuing without UI progress",
                    source_file or "<unknown>",
                )
                active_progress_callback = None

        if throttle_seconds:
            time.sleep(throttle_seconds)

    return finalize_results(results)


def finalize_results(results):
    """Apply the existing status normalization and result ordering."""
    df_results = pd.DataFrame(results)

    if "Venditori totali" in df_results.columns:
        for idx in df_results.index:
            asin = (
                str(df_results.at[idx, "ASIN"])
                if "ASIN" in df_results.columns
                else ""
            )
            venditori = df_results.at[idx, "Venditori totali"]

            if asin in ["", "None", "nan"]:
                df_results.at[idx, "Stato"] = "NON TROVATO SU AMAZON"
            elif pd.isna(venditori) or venditori == 0:
                df_results.at[idx, "Stato"] = "TROVATO SENZA OFFERTE"
            else:
                df_results.at[idx, "Stato"] = ELIGIBLE_STATUS

    if "Score" in df_results.columns:
        df_results["Score"] = pd.to_numeric(
            df_results["Score"],
            errors="coerce",
        ).fillna(0)
        df_results["BSR Beauty"] = pd.to_numeric(
            df_results["BSR Beauty"],
            errors="coerce",
        )
        df_results = df_results.sort_values(
            by=["Score", "BSR Beauty"],
            ascending=[False, True],
            na_position="last",
        )

    return df_results


def summarize_results(df_results):
    """Return lightweight counters without serializing result rows to the UI."""
    total = len(df_results)
    eligible = 0
    if "Stato" in df_results.columns:
        eligible = int((df_results["Stato"] == ELIGIBLE_STATUS).sum())

    return {
        "total": total,
        "eligible": eligible,
        "not_eligible": total - eligible,
    }


def write_results_excel(df_results, output_file):
    """Write the existing workbook format atomically, independently of the UI."""
    output_path = os.path.abspath(output_file)
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    temporary_file = tempfile.NamedTemporaryFile(
        prefix=".glowup_scout_output_",
        suffix=".xlsx",
        dir=output_dir,
        delete=False,
    )
    temporary_path = temporary_file.name
    temporary_file.close()

    logger.info("WRITING EXCEL | file=%s", output_path)

    try:
        with pd.ExcelWriter(temporary_path, engine="openpyxl") as writer:
            df_results.to_excel(writer, index=False, sheet_name="Risultati")

            ws = writer.sheets["Risultati"]
            headers = {
                ws.cell(row=1, column=col).value: col
                for col in range(1, ws.max_column + 1)
            }

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

        os.replace(temporary_path, output_path)
    except Exception:
        logger.exception(
            "EXCEL GENERATION FAILED | phase=WRITING EXCEL file=%s",
            output_path,
        )
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise

    logger.info("EXCEL GENERATED | file=%s", output_path)
    return output_path
