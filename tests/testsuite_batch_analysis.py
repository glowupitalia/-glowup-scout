import tempfile
import unittest
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from batch_analysis import (
    analyze_products,
    summarize_results,
    write_results_excel,
)


class BatchAnalysisTests(unittest.TestCase):
    def test_analysis_keeps_results_when_progress_ui_fails(self):
        df_input = pd.DataFrame({
            "EAN": ["111", "222", "333"],
            "Costo": [10, 20, 30],
        })
        catalogs = {
            "111": {
                "ASIN": "ASIN111",
                "Titolo": "Prodotto 1",
                "Brand": "Brand",
                "Categoria": "Beauty",
                "BSR Beauty": 1000,
            },
            "222": None,
            "333": {
                "ASIN": "ASIN333",
                "Titolo": "Prodotto 3",
                "Brand": "Brand",
                "Categoria": "Beauty",
                "BSR Beauty": 5000,
            },
        }
        pricing = {
            "ASIN111": {
                "Buy Box": "12 EUR",
                "Venditori totali": 2,
                "Venditori FBA": 1,
                "Venditori FBM": 1,
                "Prezzo minimo FBA": "12 EUR",
                "Prezzo minimo FBM": "13 EUR",
            },
            "ASIN333": {
                "Buy Box": "",
                "Venditori totali": 0,
                "Venditori FBA": 0,
                "Venditori FBM": 0,
                "Prezzo minimo FBA": "",
                "Prezzo minimo FBM": "",
            },
        }
        progress_calls = []

        def failing_progress(value):
            progress_calls.append(value)
            raise RuntimeError("frontend unavailable")

        result = analyze_products(
            df_input=df_input,
            costo_col="Costo",
            token="token",
            search_catalog=lambda ean, _: catalogs[ean],
            search_pricing=lambda asin, _: pricing[asin],
            safe_call=lambda function, *args: function(*args),
            progress_callback=failing_progress,
            throttle_seconds=0,
            source_file="input.xlsx",
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(progress_calls, [1 / 3])
        self.assertEqual(
            result.loc[result["EAN"] == "111", "Stato"].item(),
            "TROVATO CON OFFERTE",
        )
        self.assertEqual(
            result.loc[result["EAN"] == "222", "Stato"].item(),
            "NON TROVATO SU AMAZON",
        )
        self.assertEqual(
            result.loc[result["EAN"] == "333", "Stato"].item(),
            "TROVATO SENZA OFFERTE",
        )
        self.assertEqual(result.iloc[0]["EAN"], "111")
        self.assertEqual(
            summarize_results(result),
            {"total": 3, "eligible": 1, "not_eligible": 2},
        )

    def test_excel_keeps_sheet_and_hyperlinks(self):
        results = pd.DataFrame([{
            "EAN": "111",
            "Link Amazon": "https://www.amazon.it/dp/ASIN111",
            "Link Offerte": (
                "https://www.amazon.it/gp/offer-listing/ASIN111"
            ),
            "Stato": "TROVATO CON OFFERTE",
        }])

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "glowup_scout_output.xlsx"
            generated_path = write_results_excel(results, output_path)

            self.assertEqual(generated_path, str(output_path))
            self.assertTrue(output_path.is_file())

            workbook = load_workbook(output_path)
            worksheet = workbook["Risultati"]
            headers = {
                worksheet.cell(row=1, column=column).value: column
                for column in range(1, worksheet.max_column + 1)
            }
            amazon_cell = worksheet.cell(
                row=2,
                column=headers["Link Amazon"],
            )
            offers_cell = worksheet.cell(
                row=2,
                column=headers["Link Offerte"],
            )

            self.assertEqual(
                amazon_cell.hyperlink.target,
                results.iloc[0]["Link Amazon"],
            )
            self.assertEqual(
                offers_cell.hyperlink.target,
                results.iloc[0]["Link Offerte"],
            )


if __name__ == "__main__":
    unittest.main()
