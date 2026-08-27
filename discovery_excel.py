"""Interactive supplier-neutral multi-scenario Discovery Excel export."""

from __future__ import annotations

import math
import os
import tempfile
from decimal import Decimal

from openpyxl import Workbook
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, Font, PatternFill, Protection
from openpyxl.utils import get_column_letter

from discovery import normalize_discovery_state
from purchase_scenarios import (
    recommended_combination,
    scenario_requirement_label,
)


OPPORTUNITY_COLUMNS = [
    "EAN", "Brand", "Titolo", "Fornitore", "Scenario", "Requisito", "Costo",
    "ASIN raccomandato", "BSR Beauty", "Prezzo riferimento", "Venditori FBA", "Venditori totali",
    "Margine attuale %", "Prezzo 15%", "Prezzo 20%", "Prezzo 25%",
    "Score", "Opportunità", "Numero scenari acquisto", "Numero pagine Amazon",
    "Link Offerte Amazon",
]
SCENARIO_COLUMNS = [
    "EAN", "Brand", "Titolo", "ASIN", "Fornitore", "Seller alias", "Scenario",
    "Requisito", "Costo", "Prezzo riferimento", "BSR Beauty", "Venditori FBA",
    "Venditori totali", "Margine attuale %", "Prezzo 15%", "Prezzo 20%",
    "Prezzo 25%", "Score", "Opportunità", "Ruolo", "Stato", "Stock",
    "Lead time / Snapshot / freshness", "Warehouse", "Disponibilità",
]
TECHNICAL_COLUMNS = [
    "ObservationRowID", "ProductRowID", "ASIN", "BSR Beauty", "Prezzo riferimento",
    "Venditori FBA", "Venditori totali", "FBA fee netta",
    "FBA fee IVA inclusa", "Referral Fee", "Referral rate",
    "Referral source", "Price source", "Seller count source", "Observed at",
    "Prezzo minimo FBA", "Prezzo minimo FBM",
]
ALL_RESULTS_COLUMNS = [
    "EAN", "Brand", "Titolo", "Fornitori disponibili",
    "Numero scenari acquisto", "ASIN", "Titolo Amazon", "Compatibility",
    "Beauty", "BSR Beauty", "Prezzo riferimento", "Origine prezzo",
    "Min FBA", "Min FBM", "Venditori FBA", "Venditori totali",
    "Miglior costo disponibile", "Miglior margine disponibile %",
    "Score migliore disponibile", "Stato", "Motivo esclusione",
    "Soglia / limite rilevante", "Dettaglio",
]
LISTING_COLUMNS = [
    "EAN", "ASIN", "Titolo Amazon", "Brand Amazon",
    "Compatibility status", "Compatibility reason", "Beauty",
    "Display group", "BSR Beauty", "Catalog status", "Buy Box", "Min FBA",
    "Min FBM", "Price source", "Venditori FBA", "Venditori totali",
    "Pricing status", "Competition status", "Fee status", "Fee attempts",
    "Exclusion reason", "Link Amazon",
]
RUN_COLUMNS = ["Parametro", "Valore"]
# Backwards-compatible import name; Discovery now exports Opportunita, not Risultati.
DISCOVERY_COLUMNS = OPPORTUNITY_COLUMNS


def _value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _as_decimal(value):
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _export_context(payload):
    if isinstance(payload, dict):
        state = normalize_discovery_state(payload)
        return state, list(state.get("candidates") or []), list(state.get("results") or [])
    products = list(payload or [])
    return {}, products, products


def _product_ean(product):
    return product.get("canonical_ean") or product.get("gtin")


def _supplier_names(product):
    values = []
    for scenario in product.get("scenarios") or []:
        supplier = str(scenario.get("supplier") or "").strip()
        if supplier and supplier.casefold() not in {row.casefold() for row in values}:
            values.append(supplier)
    return " · ".join(row.upper() for row in values)


def _listing_combinations(product, listing):
    asin = listing.get("asin")
    observation_id = listing.get("amazon_observation_id")
    return [
        row for row in product.get("opportunity_combinations") or []
        if (
            (observation_id and row.get("amazon_observation_id") == observation_id)
            or (not observation_id and asin and row.get("asin") == asin)
        )
    ]


def _best_combination(rows):
    if not rows:
        return None
    return min(rows, key=lambda row: (
        -int(row.get("score") or 0),
        -float(_as_decimal(row.get("margin_percent")) or Decimal("-999999")),
        -float(_as_decimal(row.get("profit")) or Decimal("-999999")),
        float(_as_decimal(row.get("cost_gross_unit_eur")) or Decimal("999999")),
        str(row.get("combination_id") or ""),
    ))


def _best_cost(product):
    values = [
        _as_decimal(row.get("cost_gross_unit_eur"))
        for row in product.get("scenarios") or []
    ]
    values = [row for row in values if row is not None and row > 0]
    return min(values) if values else None


def _status_detail(product, listing, filters, final_product_keys):
    """Return technical status, Italian label, reason, threshold and detail."""
    if listing is None:
        catalog_status = product.get("catalog_status")
        if catalog_status == "not_found":
            return (
                "catalog_not_found", "Non trovato su Amazon",
                "Nessun listing Amazon restituito per l'EAN", "—",
                "catalog_status=not_found",
            )
        if catalog_status == "catalog_incomplete":
            return (
                "catalog_incomplete", "Catalog Amazon incompleto",
                "La paginazione Catalog non è stata completata; risultato non definitivo",
                "Ripetere Catalog Items", "catalog_status=catalog_incomplete",
            )
        return (
            "economics_unavailable", "Economia non disponibile",
            "Nessun listing Amazon valutabile", "—",
            f"catalog_status={catalog_status or 'unknown'}",
        )

    evaluation = listing.get("evaluation_status")
    compatibility = listing.get("compatibility_status")
    if compatibility == "incompatible" or evaluation == "catalog_incompatible":
        reason = listing.get("compatibility_reason") or listing.get("exclusion_reason")
        return (
            "incompatible_listing", "Listing incompatibile",
            "Listing non compatibile con il prodotto supplier", "Compatibilità prodotto",
            _detail_text(reason),
        )
    if evaluation == "beauty_filtered":
        return (
            "not_beauty", "Non Beauty",
            "Display group Amazon non riconosciuto come Beauty", "Beauty richiesta",
            _detail_text(listing.get("beauty_status") or listing.get("display_group")),
        )
    if evaluation == "bsr_filtered":
        bsr = listing.get("bsr_beauty")
        minimum = filters.get("bsr_min")
        maximum = filters.get("bsr_max")
        if bsr is None or _as_decimal(bsr) is None or _as_decimal(bsr) <= 0:
            reason = "BSR Beauty mancante o non valido"
        elif minimum is not None and _as_decimal(bsr) < _as_decimal(minimum):
            reason = f"BSR {_integer_text(bsr)} < minimo {_integer_text(minimum)}"
        else:
            reason = f"BSR {_integer_text(bsr)} > massimo {_integer_text(maximum)}"
        return (
            "bsr_out_of_range", "Fuori range BSR", reason,
            f"BSR {minimum}–{maximum}", _detail_text(listing.get("exclusion_reason")),
        )
    if evaluation == "competition_filtered":
        reasons = listing.get("exclusion_reasons") or str(
            listing.get("exclusion_reason") or ""
        ).split(",")
        descriptions = []
        if "fba_sellers_above_threshold" in reasons:
            descriptions.append(
                f"Venditori FBA {_integer_text(listing.get('fba_sellers'))} > "
                f"limite {_integer_text(filters.get('max_fba_sellers'))}"
            )
        if "total_sellers_above_threshold" in reasons:
            descriptions.append(
                f"Venditori totali {_integer_text(listing.get('total_sellers'))} > "
                f"limite {_integer_text(filters.get('max_total_sellers'))}"
            )
        if "missing_reference_price" in reasons:
            descriptions.append("Prezzo riferimento non disponibile")
        return (
            "competition_filtered", "Escluso per concorrenza",
            "; ".join(descriptions) or "Concorrenza oltre i limiti configurati",
            f"FBA ≤ {filters.get('max_fba_sellers')} · Totali ≤ {filters.get('max_total_sellers')}",
            _detail_text(reasons),
        )
    if listing.get("fee_status") == "fee_pending":
        return (
            "fee_pending", "Fee in attesa", "Product Fees temporaneamente non disponibile",
            "Retry Product Fees", _detail_text(listing.get("fee_error")),
        )
    if listing.get("fee_status") in {"invalid", "fee_invalid"}:
        return (
            "fee_invalid", "Fee non valida", "Product Fees non valida",
            "Fee Estimate valida richiesta", _detail_text(listing.get("fee_error")),
        )

    combinations = _listing_combinations(product, listing)
    passed = [row for row in combinations if row.get("evaluation_status") == "margin_passed"]
    product_key = product.get("product_key")
    if passed and product_key in final_product_keys:
        return (
            "opportunity", "Opportunità", "Almeno una combinazione supera il margine minimo",
            f"Margine ≥ {float(filters.get('minimum_margin') or 0):.2f}%", "",
        )
    if combinations:
        best = _best_combination(combinations)
        margin = _as_decimal((best or {}).get("margin_percent"))
        threshold = _as_decimal(filters.get("minimum_margin")) or Decimal("0")
        if margin is not None and margin < threshold:
            return (
                "margin_below_threshold", "Sotto soglia margine",
                f"Margine {float(margin):.2f}% < soglia {float(threshold):.2f}%",
                f"Margine ≥ {float(threshold):.2f}%", "",
            )
    return (
        "economics_unavailable", "Economia non disponibile",
        "Il listing non dispone di una valutazione economica completa", "—",
        _detail_text(listing.get("exclusion_reason") or evaluation),
    )


def _detail_text(value):
    if isinstance(value, (list, tuple, set)):
        return " · ".join(str(row) for row in value if row not in (None, ""))
    if isinstance(value, dict):
        return " · ".join(f"{key}={item}" for key, item in sorted(value.items()))
    return str(value or "")


def _integer_text(value):
    number = _as_decimal(value)
    return f"{int(number):,}".replace(",", ".") if number is not None else "—"


def _lookup(row, observation_id_column, data_column, last_row):
    return (
        f"INDEX('Dati'!${data_column}$2:${data_column}${last_row},"
        f"MATCH(${observation_id_column}{row},'Dati'!$A$2:$A${last_row},0))"
    )


def _economic_formulas(row, *, cost_col, margin_col, target_cols, score_col,
                       opportunity_col, observation_id_col, last_data_row):
    price = _lookup(row, observation_id_col, "E", last_data_row)
    bsr = _lookup(row, observation_id_col, "D", last_data_row)
    fba_sellers = _lookup(row, observation_id_col, "F", last_data_row)
    total_sellers = _lookup(row, observation_id_col, "G", last_data_row)
    fba_gross = _lookup(row, observation_id_col, "I", last_data_row)
    referral_rate = _lookup(row, observation_id_col, "K", last_data_row)
    margin = (
        f'=IFERROR(IF(AND(ISNUMBER({price}),{price}>0,ISNUMBER({cost_col}{row}),'
        f'ISNUMBER({fba_gross}),ISNUMBER({referral_rate})),'
        f'ROUND(({price}-{cost_col}{row}-({price}*{referral_rate})-'
        f'{fba_gross})/{price},4),""),"")'
    )
    targets = {}
    for target, column in zip((0.15, 0.20, 0.25), target_cols):
        targets[column] = (
            f'=IFERROR(IF(AND(ISNUMBER({cost_col}{row}),ISNUMBER({fba_gross}),'
            f'ISNUMBER({referral_rate}),(1-{referral_rate}-{target})>0),'
            f'ROUND(({cost_col}{row}+{fba_gross})/(1-{referral_rate}-{target}),2),""),"")'
        )
    bsr_points = (
        f"IF(AND(ISNUMBER({bsr}),{bsr}>=0),IF({bsr}<=1000,50,"
        f"IF({bsr}<=5000,45,IF({bsr}<=10000,40,IF({bsr}<=25000,30,"
        f"IF({bsr}<=50000,15,5))))),0)"
    )
    fba_points = (
        f"IF(AND(ISNUMBER({total_sellers}),{total_sellers}>0,ISNUMBER({fba_sellers}),"
        f"{fba_sellers}>=0),IF({fba_sellers}=0,20,IF({fba_sellers}<=2,18,"
        f"IF({fba_sellers}<=4,14,IF({fba_sellers}<=6,10,IF({fba_sellers}<=10,5,0))))),0)"
    )
    total_points = (
        f"IF(AND(ISNUMBER({total_sellers}),{total_sellers}>0),"
        f"IF({total_sellers}<=3,10,IF({total_sellers}<=6,7,"
        f"IF({total_sellers}<=10,4,0))),0)"
    )
    margin_points = (
        f"IF(ISNUMBER({margin_col}{row}),IF({margin_col}{row}<10%,0,"
        f"IF({margin_col}{row}<15%,4,IF({margin_col}{row}<20%,14,"
        f"IF({margin_col}{row}<25%,18,20)))),0)"
    )
    score = f"={bsr_points}+{fba_points}+{total_points}+{margin_points}"
    opportunity = (
        f'=IF({score_col}{row}>=85,"🟢 Eccellente",IF({score_col}{row}>=70,'
        f'"🟢 Ottima",IF({score_col}{row}>=55,"🟡 Interessante",'
        f'IF({score_col}{row}>=40,"🟠 Da valutare","🔴 Debole"))))'
    )
    return margin, targets, score, opportunity


def _recommended(product):
    scenario_id = (product.get("scenario_roles") or {}).get("scenario_raccomandato")
    return next(
        (row for row in product.get("scenarios") or [] if row.get("scenario_id") == scenario_id),
        None,
    )


def _observations(product):
    rows = list(product.get("amazon_observations") or [])
    legacy = product.get("amazon_observation") or {}
    if legacy and not any(
        row.get("observation_id") == legacy.get("observation_id") for row in rows
    ):
        rows.append(legacy)
    return rows


def _combinations(product):
    rows = list(product.get("opportunity_combinations") or [])
    if rows:
        return rows
    observation = product.get("amazon_observation") or {}
    for scenario in product.get("scenarios") or []:
        economics = scenario.get("economics") or {}
        if not (
            scenario.get("economics_status") == "ready"
            or economics.get("status") == "ready"
            or (
                scenario.get("margin_percent") is not None
                and scenario.get("score") is not None
            )
        ):
            continue
        rows.append({
            "combination_id": f"legacy|{scenario.get('scenario_id')}",
            "scenario_id": scenario.get("scenario_id"),
            "asin": observation.get("asin"),
            "amazon_observation_id": observation.get("observation_id") or product.get("product_key"),
            "price_reference": observation.get("reference_price"),
            "margin_percent": scenario.get("margin_percent"),
            "score": scenario.get("score"), "opportunity": scenario.get("opportunity"),
            "evaluation_status": scenario.get("evaluation_status"),
            "economics": economics,
        })
    return rows


def _style_sheet(ws, *, visible_columns, cost_column, hidden_columns, data_ws):
    header_fill = PatternFill("solid", fgColor="1F4E78")
    for cell in ws[1]:
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
    ws[f"{cost_column}1"].comment = Comment(
        "Modifica questi valori per simulare margine, prezzi target, Score e Opportunità.",
        "GlowUp Scout",
    )
    editable_fill = PatternFill("solid", fgColor="EAF2FE")
    for row in range(2, ws.max_row + 1):
        ws[f"{cost_column}{row}"].protection = Protection(locked=False)
        ws[f"{cost_column}{row}"].fill = editable_fill
    for column in hidden_columns:
        ws.column_dimensions[column].hidden = True
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(visible_columns)}{ws.max_row}"
    ws.print_area = f"A1:{get_column_letter(visible_columns)}{ws.max_row}"
    ws.protection.sheet = True
    ws.protection.autoFilter = False
    ws.protection.sort = False
    ws.protection.selectUnlockedCells = False
    data_ws.sheet_state = "hidden"


def _style_audit_sheet(ws, widths, *, currency_columns=(), percent_columns=(),
                       integer_columns=(), text_columns=(), hyperlink_column=None):
    header_fill = PatternFill("solid", fgColor="1F4E78")
    for cell in ws[1]:
        cell.font = Font(color="FFFFFF", bold=True)
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(ws.max_column)}{max(1, ws.max_row)}"
    ws.sheet_view.showGridLines = False
    for index, width in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(index)].width = width
    for row in range(2, ws.max_row + 1):
        for column in text_columns:
            ws.cell(row, column).number_format = "@"
        for column in currency_columns:
            ws.cell(row, column).number_format = '€ #,##0.00'
        for column in percent_columns:
            ws.cell(row, column).number_format = '0.00%'
        for column in integer_columns:
            ws.cell(row, column).number_format = '#,##0'
        if hyperlink_column:
            cell = ws.cell(row, hyperlink_column)
            if cell.value:
                cell.hyperlink = str(cell.value)
                cell.style = "Hyperlink"


def _run_metadata(state, candidates, final_products):
    filters = state.get("filters") or {}
    funnel = state.get("funnel") or {}
    selected = state.get("selected_suppliers") or []
    if not selected:
        selected = sorted({
            str(scenario.get("supplier") or "").upper()
            for product in candidates
            for scenario in product.get("scenarios") or []
            if scenario.get("supplier")
        }) or ["QOGITA"]
    return [
        ("Job ID", state.get("job_id") or "—"),
        ("Schema checkpoint", state.get("schema_version") or "—"),
        ("Stato job", state.get("status") or "—"),
        ("Fase", state.get("phase") or "—"),
        ("Avviato", state.get("started_at") or state.get("created_at") or "—"),
        ("Completato", state.get("completed_at") or "—"),
        ("Fornitori selezionati", " · ".join(str(row).upper() for row in selected)),
        ("BSR minimo", filters.get("bsr_min")),
        ("BSR massimo", filters.get("bsr_max")),
        ("Venditori FBA massimi", filters.get("max_fba_sellers")),
        ("Venditori totali massimi", filters.get("max_total_sellers")),
        ("Margine minimo %", filters.get("minimum_margin")),
        ("EAN universo supplier", state.get("total_supplier_ean_universe")),
        ("Identificatori idonei", state.get("eligible_identifier_count")),
        ("Budget ricerca", state.get("run_budget") or "all"),
        ("Identificatori campionati", state.get("sampled_identifier_count") or len(candidates)),
        ("Strategia campionamento", state.get("sampling_strategy") or "—"),
        ("Scope rotazione", state.get("rotation_scope") or "—"),
        ("Ciclo rotazione", state.get("rotation_cycle_id")),
        ("Analizzati prima della run", state.get("rotation_analyzed_before_run")),
        ("Analizzati in questa run", state.get("rotation_analyzed_this_run")),
        ("Rimanenti nel ciclo", state.get("rotation_remaining_after_run")),
        *[
            (
                f"Freshness {str(supplier).upper()}",
                " · ".join(filter(None, (
                    str((state.get("supplier_snapshot_set") or {}).get(supplier, {}).get("freshness") or "—"),
                    str((state.get("supplier_snapshot_set") or {}).get(supplier, {}).get("snapshot_at") or "—"),
                ))),
            )
            for supplier in selected
        ],
        ("Prodotti analizzati", len(candidates)),
        ("Prodotti trovati Amazon", funnel.get("amazon_found")),
        ("Pagine Amazon trovate", funnel.get("amazon_listings_found")),
        (
            "Prodotti esclusi per concorrenza",
            funnel.get("competition_filtered_products"),
        ),
        (
            "Pagine Amazon con concorrenza valida",
            funnel.get("competition_passed_listings"),
        ),
        ("Opportunità finali", len(final_products)),
    ]


def write_discovery_excel(results, output_file):
    state, candidates, final_products = _export_context(results)
    output_path = os.path.abspath(output_file)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    descriptor, temporary_path = tempfile.mkstemp(
        prefix=".glowup_discovery_", suffix=".xlsx",
        dir=os.path.dirname(output_path) or ".",
    )
    os.close(descriptor)
    try:
        workbook = Workbook()
        opportunities = workbook.active
        opportunities.title = "Opportunità"
        all_results_ws = workbook.create_sheet("Tutti i risultati")
        listings_ws = workbook.create_sheet("Listing Amazon")
        scenarios_ws = workbook.create_sheet("Scenari")
        data_ws = workbook.create_sheet("Dati")
        run_ws = workbook.create_sheet("Parametri run")
        opportunities.append(OPPORTUNITY_COLUMNS + [
            "ProductRowID", "ScenarioRowID", "ObservationRowID", "CombinationRowID"
        ])
        all_results_ws.append(ALL_RESULTS_COLUMNS)
        listings_ws.append(LISTING_COLUMNS)
        scenarios_ws.append(SCENARIO_COLUMNS + [
            "ProductRowID", "ScenarioRowID", "ObservationRowID", "CombinationRowID"
        ])
        data_ws.append(TECHNICAL_COLUMNS)
        run_ws.append(RUN_COLUMNS)

        observations = {}
        for observation in state.get("amazon_observations") or []:
            observation_id = observation.get("observation_id")
            if observation_id:
                product_keys = (observation.get("diagnostics") or {}).get("product_keys") or []
                observations.setdefault(
                    observation_id, (product_keys[0] if product_keys else "", observation)
                )
        for product in candidates:
            product_id = product.get("product_key") or f"product-{product.get('gtin')}"
            for observation in _observations(product):
                observation_id = observation.get("observation_id") or product_id
                observations.setdefault(observation_id, (product_id, observation))
        final_product_keys = {
            row.get("product_key") for row in final_products if row.get("product_key")
        }

        for product in final_products:
            product_id = product.get("product_key") or f"product-{_product_ean(product)}"
            recommended_pair = recommended_combination(product)
            scenario_by_id = {
                row.get("scenario_id"): row for row in product.get("scenarios") or []
            }
            recommended = scenario_by_id.get(
                (recommended_pair or {}).get("scenario_id")
            ) or _recommended(product)
            if not recommended:
                continue
            if recommended_pair:
                observation = next(
                    (row for row in _observations(product) if row.get("observation_id") == recommended_pair.get("amazon_observation_id")),
                    product.get("amazon_observation") or {},
                )
            else:
                observation = product.get("amazon_observation") or {}
                recommended_pair = {
                    "combination_id": f"legacy|{recommended.get('scenario_id')}",
                    "scenario_id": recommended.get("scenario_id"),
                    "asin": observation.get("asin"),
                    "amazon_observation_id": observation.get("observation_id") or product_id,
                }
            opportunities.append([
                _product_ean(product), observation.get("amazon_brand") or product.get("brand"),
                observation.get("amazon_title") or product.get("title"),
                str(recommended.get("supplier") or "").upper(),
                recommended.get("scenario_label"), scenario_requirement_label(recommended),
                _value(recommended.get("cost_gross_unit_eur")), recommended_pair.get("asin"),
                observation.get("bsr_beauty"),
                _value(observation.get("reference_price")), observation.get("fba_sellers"),
                observation.get("total_sellers"), None, None, None, None, None, None,
                len(product.get("scenarios") or []), len(product.get("amazon_listings") or [observation]),
                product.get("amazon_offers_url"), product_id,
                recommended.get("scenario_id"), recommended_pair.get("amazon_observation_id"),
                recommended_pair.get("combination_id"),
            ])

        for product in candidates:
            product_id = product.get("product_key") or f"product-{_product_ean(product)}"
            recommended_pair = recommended_combination(product)
            if not recommended_pair:
                legacy_recommended = _recommended(product) or {}
                legacy_observation = product.get("amazon_observation") or {}
                recommended_pair = {
                    "combination_id": f"legacy|{legacy_recommended.get('scenario_id')}",
                    "scenario_id": legacy_recommended.get("scenario_id"),
                    "asin": legacy_observation.get("asin"),
                    "amazon_observation_id": (
                        legacy_observation.get("observation_id") or product_id
                    ),
                }
            scenario_by_id = {row.get("scenario_id"): row for row in product.get("scenarios") or []}
            observation_by_id = {row.get("observation_id"): row for row in _observations(product)}
            for combination in _combinations(product):
                scenario = scenario_by_id.get(combination.get("scenario_id"), {})
                combination_observation = observation_by_id.get(
                    combination.get("amazon_observation_id"), {}
                )
                if not combination_observation:
                    combination_observation = observations.get(
                        combination.get("amazon_observation_id"), (None, {})
                    )[1]
                scenarios_ws.append([
                    _product_ean(product), combination_observation.get("amazon_brand") or product.get("brand"),
                    combination_observation.get("amazon_title") or product.get("title"),
                    combination.get("asin"),
                    str(scenario.get("supplier") or "").upper(),
                    scenario.get("supplier_alias"), scenario.get("scenario_label"),
                    scenario_requirement_label(scenario), _value(scenario.get("cost_gross_unit_eur")),
                    _value(combination_observation.get("reference_price")),
                    combination_observation.get("bsr_beauty"),
                    combination_observation.get("fba_sellers"),
                    combination_observation.get("total_sellers"),
                    None, None, None, None, None, None,
                    "Raccomandata" if combination.get("combination_id") == recommended_pair.get("combination_id") else "",
                    combination.get("evaluation_status"), scenario.get("stock"),
                    f"{scenario.get('lead_time') or '—'} · "
                    f"{scenario.get('snapshot_at') or ''} · "
                    f"{scenario.get('freshness_status') or ''}",
                    scenario.get("warehouse"),
                    scenario.get("availability_text")
                    or scenario.get("availability_status"),
                    product_id, scenario.get("scenario_id"),
                    combination.get("amazon_observation_id"), combination.get("combination_id"),
                ])

            listings = list(product.get("amazon_listings") or [])
            if not listings:
                technical, label, reason, threshold, detail = _status_detail(
                    product, None, state.get("filters") or {}, final_product_keys,
                )
                all_results_ws.append([
                    _product_ean(product), product.get("brand"), product.get("title"),
                    _supplier_names(product), len(product.get("scenarios") or []),
                    None, None, None, None, None, None, None, None, None, None,
                    None, _value(_best_cost(product)), None, None, label, reason,
                    threshold, f"{technical} · {detail}".strip(" ·"),
                ])
            for listing in listings:
                combinations = _listing_combinations(product, listing)
                best = _best_combination(combinations)
                technical, label, reason, threshold, detail = _status_detail(
                    product, listing, state.get("filters") or {}, final_product_keys,
                )
                all_results_ws.append([
                    _product_ean(product), product.get("brand"), product.get("title"),
                    _supplier_names(product), len(product.get("scenarios") or []),
                    listing.get("asin"), listing.get("title"),
                    listing.get("compatibility_status"),
                    "Sì" if listing.get("beauty_status") == "display_group_beauty" else "No",
                    listing.get("bsr_beauty"), _value(listing.get("reference_price")),
                    listing.get("price_source"), _value(listing.get("min_fba_price")),
                    _value(listing.get("min_fbm_price")), listing.get("fba_sellers"),
                    listing.get("total_sellers"), _value(_best_cost(product)),
                    (_value(_as_decimal((best or {}).get("margin_percent")) / Decimal("100"))
                     if (best or {}).get("margin_percent") is not None else None),
                    (best or {}).get("score"), label, reason, threshold,
                    f"{technical} · {detail}".strip(" ·"),
                ])
                link = (
                    f"https://www.amazon.it/gp/offer-listing/{listing.get('asin')}"
                    if listing.get("asin") else None
                )
                buy_box = (
                    listing.get("reference_price")
                    if listing.get("price_source") == "buy_box" else listing.get("buy_box_price")
                )
                listings_ws.append([
                    _product_ean(product), listing.get("asin"), listing.get("title"),
                    listing.get("brand"), listing.get("compatibility_status"),
                    _detail_text(listing.get("compatibility_reason")),
                    "Sì" if listing.get("beauty_status") == "display_group_beauty" else "No",
                    listing.get("display_group"), listing.get("bsr_beauty"),
                    listing.get("catalog_status"), _value(buy_box),
                    _value(listing.get("min_fba_price")), _value(listing.get("min_fbm_price")),
                    listing.get("price_source"), listing.get("fba_sellers"),
                    listing.get("total_sellers"), listing.get("pricing_status"),
                    listing.get("competition_status"), listing.get("fee_status"),
                    listing.get("fee_attempts"),
                    listing.get("exclusion_reason") or reason, link,
                ])

        for observation_id, (product_id, observation) in observations.items():
            fee = observation.get("fee_estimate") or {}
            data_ws.append([
                observation_id, product_id, observation.get("asin"), observation.get("bsr_beauty"),
                _value(observation.get("reference_price")), observation.get("fba_sellers"),
                observation.get("total_sellers"), _value(fee.get("fba_fee_net")),
                _value(fee.get("fba_fee_gross")), _value(fee.get("referral_fee")),
                _value(observation.get("referral_rate") or fee.get("referral_rate")),
                observation.get("referral_source"),
                observation.get("price_source"), observation.get("seller_count_source"),
                observation.get("observed_at"),
                _value(observation.get("min_fba_price")),
                _value(observation.get("min_fbm_price")),
            ])
        for label, value in _run_metadata(state, candidates, final_products):
            run_ws.append([label, _value(value)])
        last_data_row = max(2, data_ws.max_row)

        for row in range(2, opportunities.max_row + 1):
            margin, targets, score, opportunity = _economic_formulas(
                row, cost_col="G", margin_col="M", target_cols=("N", "O", "P"),
                score_col="Q", opportunity_col="R", observation_id_col="X",
                last_data_row=last_data_row,
            )
            opportunities[f"M{row}"] = margin
            opportunities[f"N{row}"] = targets["N"]
            opportunities[f"O{row}"] = targets["O"]
            opportunities[f"P{row}"] = targets["P"]
            opportunities[f"Q{row}"] = score
            opportunities[f"R{row}"] = opportunity
            opportunities[f"M{row}"].number_format = "0.00%"
            for column in ("G", "J", "N", "O", "P"):
                opportunities[f"{column}{row}"].number_format = '€ #,##0.00'
            link = opportunities[f"U{row}"]
            if link.value:
                link.hyperlink = link.value
                link.style = "Hyperlink"

        for row in range(2, scenarios_ws.max_row + 1):
            margin, targets, score, opportunity = _economic_formulas(
                row, cost_col="I", margin_col="N", target_cols=("O", "P", "Q"),
                score_col="R", opportunity_col="S", observation_id_col="AB",
                last_data_row=last_data_row,
            )
            scenarios_ws[f"N{row}"] = margin
            scenarios_ws[f"O{row}"] = targets["O"]
            scenarios_ws[f"P{row}"] = targets["P"]
            scenarios_ws[f"Q{row}"] = targets["Q"]
            scenarios_ws[f"R{row}"] = score
            scenarios_ws[f"S{row}"] = opportunity
            scenarios_ws[f"N{row}"].number_format = "0.00%"
            for column in ("I", "J", "O", "P", "Q"):
                scenarios_ws[f"{column}{row}"].number_format = '€ #,##0.00'

        _style_sheet(
            opportunities, visible_columns=len(OPPORTUNITY_COLUMNS), cost_column="G",
            hidden_columns=("V", "W", "X", "Y"), data_ws=data_ws,
        )
        _style_sheet(
            scenarios_ws, visible_columns=len(SCENARIO_COLUMNS), cost_column="I",
            hidden_columns=("Z", "AA", "AB", "AC"), data_ws=data_ws,
        )
        _style_audit_sheet(
            all_results_ws,
            [16, 20, 42, 28, 14, 14, 44, 18, 10, 12, 16, 16, 12, 12,
             14, 15, 18, 18, 16, 24, 48, 28, 42],
            currency_columns=(11, 13, 14, 17), percent_columns=(18,),
            integer_columns=(5, 10, 15, 16, 19), text_columns=(1, 6),
        )
        _style_audit_sheet(
            listings_ws,
            [16, 14, 46, 20, 20, 42, 10, 32, 12, 18, 12, 12, 12, 16,
             14, 15, 18, 20, 16, 12, 44, 42],
            currency_columns=(11, 12, 13), integer_columns=(9, 15, 16, 20),
            text_columns=(1, 2), hyperlink_column=22,
        )
        _style_audit_sheet(run_ws, [28, 54])
        run_ws.auto_filter.ref = None
        run_ws.freeze_panes = "A2"
        opportunity_widths = [16, 20, 45, 12, 22, 18, 12, 14, 12, 18, 14, 15, 17, 12, 12, 12, 9, 20, 16, 16, 42]
        scenario_widths = [16, 20, 45, 14, 12, 16, 22, 18, 12, 18, 12, 14, 15, 17, 12, 12, 12, 9, 20, 16, 20, 10, 32, 14, 20]
        for ws, widths in ((opportunities, opportunity_widths), (scenarios_ws, scenario_widths)):
            for index, width in enumerate(widths, start=1):
                ws.column_dimensions[get_column_letter(index)].width = width
            ws.sheet_view.showGridLines = False
            for row in range(2, ws.max_row + 1):
                ws.cell(row, 1).number_format = "@"
                if ws is opportunities:
                    ws.cell(row, 8).number_format = "@"
                else:
                    ws.cell(row, 4).number_format = "@"
        data_ws.freeze_panes = "A2"
        data_ws.auto_filter.ref = f"A1:{get_column_letter(data_ws.max_column)}{max(1, data_ws.max_row)}"

        workbook.calculation.calcMode = "auto"
        workbook.calculation.fullCalcOnLoad = True
        workbook.calculation.forceFullCalc = True
        workbook.calculation.calcOnSave = True
        workbook.save(temporary_path)
        os.replace(temporary_path, output_path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise
    return output_path
