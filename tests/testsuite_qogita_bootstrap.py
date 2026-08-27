import json
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from qogita_bootstrap import (
    PRODUCT_LINK_SOURCE,
    QogitaBootstrapClient,
    QogitaBootstrapError,
    QogitaBootstrapStore,
    QogitaFidConflict,
    SharedQogitaAuth,
    parse_product_link_redirect,
    qogita_scenarios_from_offers,
    run_qogita_bootstrap,
    run_qogita_bootstrap_concurrent,
)
from qogita_catalog_pipeline import QogitaCatalogPipelineStore
from supplier_catalog import SupplierCatalogStore
from supplier_catalog_collectors import QogitaCatalogExportReader, QOGITA_EXPORT_COLUMNS


class BootstrapFakeClient:
    def __init__(self, *, fail_gtins=()):
        self.fail_gtins = set(fail_gtins)
        self.resolve_calls = []
        self.offer_calls = []
        self.metrics = {
            "login_requests": 0, "product_link_requests": 0, "offers_requests": 0,
            "retries": 0, "http_429": 0, "http_5xx": 0,
            "resolver_elapsed_seconds": 0.0, "offers_elapsed_seconds": 0.0,
        }

    def close(self):
        return None

    def resolve_fid(self, gtin, product_url=None):
        self.resolve_calls.append(gtin)
        self.metrics["product_link_requests"] += 1
        self.metrics["resolver_elapsed_seconds"] += 0.01
        if gtin in self.fail_gtins:
            raise QogitaBootstrapError("not found", code="resolver_unexpected_http")
        return {"variant_fid": f"FID{gtin[-4:]}", "elapsed_seconds": 0.01}

    def fetch_offers(self, fid):
        self.offer_calls.append(fid)
        self.metrics["offers_requests"] += 1
        self.metrics["offers_elapsed_seconds"] += 0.02
        return {"elapsed_seconds": 0.02, "payload": {
            "offers": [{
                "qid": f"OFFER-{fid}", "seller": "seller-a", "unit": 1,
                "inventory": 10, "estimatedDeliveryTime": 3,
                "tieredPrices": [
                    {"tierMov": {"amount": "500", "currency": "EUR"},
                     "tierPrice": {"amount": "5.00", "currency": "EUR"}},
                    {"tierMov": {"amount": "1500", "currency": "EUR"},
                     "tierPrice": {"amount": "4.50", "currency": "EUR"}},
                ],
            }],
        }}


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self):
        return self._payload


class ExpiringSession:
    def __init__(self):
        self.tokens = iter(("token-one", "token-two"))
        self.authorization = []

    def post(self, *args, **kwargs):
        return FakeResponse(200, {"access": next(self.tokens)})

    def get(self, *args, **kwargs):
        self.authorization.append(kwargs["headers"]["Authorization"])
        if len(self.authorization) == 1:
            return FakeResponse(401)
        return FakeResponse(200, {"offers": []})


def valid_gtin(index):
    prefix = f"{index:012d}"
    total = sum(int(char) * (1 if position % 2 == 0 else 3)
                for position, char in enumerate(prefix))
    return prefix + str((10 - total % 10) % 10)


class QogitaBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "catalog.sqlite3"
        self.catalog = SupplierCatalogStore(self.database)
        self.pipeline = QogitaCatalogPipelineStore(self.database)
        self.store = QogitaBootstrapStore(self.database)

    def tearDown(self):
        self.temporary.cleanup()

    def staging(self, count=6):
        path = self.root / "catalog.csv"
        with path.open("w", encoding="utf-8") as handle:
            handle.write(",".join(QOGITA_EXPORT_COLUMNS) + "\n")
            for index in range(1, count + 1):
                gtin = valid_gtin(index)
                handle.write(
                    f"{gtin},Product {index},Category,Brand,5.00,1,10,No,,1,10,"
                    f"https://api.qogita.com/variants/link/{gtin}/\n"
                )
        run_id = self.catalog.start_run(
            "qogita", coverage_type="full_account_catalog",
            coverage_description="fixture", coverage_complete=True, sampled=False,
        )
        reader = QogitaCatalogExportReader(path)
        self.catalog.publish_product_catalog_stream(
            run_id, supplier="qogita", products=reader.products(), elapsed_seconds=0,
            product_catalog_coverage_type="full_account_catalog",
            product_catalog_coverage_complete=True, promote=False,
        )
        self.pipeline.populate_enrichment_queue(run_id)
        return run_id

    def test_resolver_accepts_exact_302_contract(self):
        result = parse_product_link_redirect(
            "8809738310960", 302,
            "https://www.qogita.com/products/VPQQO6/beauty-of-joseon/",
        )
        self.assertEqual(result["variant_fid"], "VPQQO6")
        self.assertEqual(result["variant_fid_source"], PRODUCT_LINK_SOURCE)

    def test_resolver_rejects_bad_location_host_path_and_status(self):
        cases = [
            (302, None),
            (302, "https://evil.test/products/FID/slug/"),
            (302, "https://www.qogita.com/not-products/FID/slug/"),
            (302, "https://www.qogita.com/products/bad_fid/slug/"),
            (200, "https://www.qogita.com/products/FID/slug/"),
            (301, "https://www.qogita.com/products/FID/slug/"),
        ]
        for status, location in cases:
            with self.subTest(status=status, location=location):
                with self.assertRaises(QogitaBootstrapError):
                    parse_product_link_redirect("8809738310960", status, location)

    def test_offers_refreshes_expired_token_once(self):
        session = ExpiringSession()
        client = QogitaBootstrapClient(
            base_url="https://api.qogita.test", email="buyer@example.test",
            password="secret", session=session,
        )
        result = client.fetch_offers("FID")
        self.assertEqual(result["payload"], {"offers": []})
        self.assertEqual(session.authorization, ["Bearer token-one", "Bearer token-two"])
        self.assertEqual(client.metrics["login_requests"], 2)
        self.assertEqual(client.metrics["offers_requests"], 2)
        self.assertEqual(client.metrics["retries"], 1)

    def test_fid_persistence_is_idempotent_and_conflicts_are_not_overwritten(self):
        run_id = self.staging(2)
        bootstrap = self.store.create_bootstrap(run_id, target_count=2, batch_size=1)
        product = self.store.products(bootstrap["bootstrap_run_id"])[0]
        first = self.store.persist_fid(
            bootstrap["bootstrap_run_id"], product["canonical_product_key"], "FID1",
            elapsed_seconds=0.1, attempts=1,
        )
        second = self.store.persist_fid(
            bootstrap["bootstrap_run_id"], product["canonical_product_key"], "FID1",
            elapsed_seconds=0.1, attempts=1,
        )
        self.assertFalse(first["no_op"])
        self.assertTrue(second["no_op"])
        with self.assertRaises(QogitaFidConflict):
            self.store.persist_fid(
                bootstrap["bootstrap_run_id"], product["canonical_product_key"], "OTHER",
                elapsed_seconds=0.1, attempts=1,
            )
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT variant_fid,variant_fid_source,enrichment_status FROM supplier_catalog_products "
                "WHERE run_id=? AND canonical_product_key=?",
                (run_id, product["canonical_product_key"]),
            ).fetchone()
        self.assertEqual(row, ("FID1", PRODUCT_LINK_SOURCE, "enrichment_failed"))

    def test_scenario_identity_ignores_price_stock_and_timestamp(self):
        product = {"canonical_product_key": "product", "gtin": valid_gtin(1),
                   "brand": "Brand", "title": "Product", "product_url": "https://example.test"}
        payload = BootstrapFakeClient().fetch_offers("FID")["payload"]
        first, _ = qogita_scenarios_from_offers(
            product, "FID", payload, staging_run_id="stage", observed_at="2026-01-01T00:00:00Z",
        )
        payload["offers"][0]["inventory"] = 99
        payload["offers"][0]["tieredPrices"][0]["tierPrice"]["amount"] = "8.00"
        second, _ = qogita_scenarios_from_offers(
            product, "FID", payload, staging_run_id="stage", observed_at="2026-02-01T00:00:00Z",
        )
        self.assertEqual(
            {row["scenario_id"] for row in first},
            {row["scenario_id"] for row in second},
        )

    def test_batch_checkpoint_resume_and_scenario_idempotency(self):
        run_id = self.staging(6)
        bootstrap = self.store.create_bootstrap(run_id, target_count=6, batch_size=2)
        client = BootstrapFakeClient()
        first = run_qogita_bootstrap(
            bootstrap["bootstrap_run_id"], store=self.store, client=client,
            max_products=2, product_link_pacing=0, offers_pacing=0, sleep_func=lambda _: None,
        )
        self.assertEqual(first["products_attempted"], 2)
        self.assertEqual(first["offers_success"], 2)
        self.assertEqual(first["scenarios_written"], 4)
        self.assertEqual(first["completed_batches"], 1)
        second_client = BootstrapFakeClient()
        final = run_qogita_bootstrap(
            bootstrap["bootstrap_run_id"], store=self.store, client=second_client,
            max_products=4, product_link_pacing=0, offers_pacing=0, sleep_func=lambda _: None,
        )
        self.assertEqual(final["products_attempted"], 6)
        self.assertEqual(final["offers_success"], 6)
        self.assertEqual(final["scenarios_written"], 12)
        self.assertEqual(final["completed_batches"], 3)
        self.assertEqual(len(second_client.resolve_calls), 4)
        with sqlite3.connect(self.database) as connection:
            scenarios = connection.execute(
                "SELECT COUNT(*),COUNT(DISTINCT scenario_id) FROM supplier_catalog_scenarios WHERE run_id=?",
                (run_id,),
            ).fetchone()
        self.assertEqual(scenarios, (12, 12))
        again = run_qogita_bootstrap(
            bootstrap["bootstrap_run_id"], store=self.store, client=BootstrapFakeClient(),
            max_products=6, product_link_pacing=0, offers_pacing=0, sleep_func=lambda _: None,
        )
        self.assertEqual(again["invocation_products_attempted"], 0)
        self.assertIsNone(self.catalog.active_generation_metadata("qogita"))

    def test_fid_resolved_offers_pending_resumes_without_resolver(self):
        run_id = self.staging(1)
        bootstrap = self.store.create_bootstrap(run_id, target_count=1, batch_size=1)
        product = self.store.products(bootstrap["bootstrap_run_id"])[0]
        self.store.persist_fid(
            bootstrap["bootstrap_run_id"], product["canonical_product_key"], "FID1",
            elapsed_seconds=0.1, attempts=1,
        )
        client = BootstrapFakeClient()
        result = run_qogita_bootstrap(
            bootstrap["bootstrap_run_id"], store=self.store, client=client,
            max_products=1, product_link_pacing=0, offers_pacing=0, sleep_func=lambda _: None,
        )
        self.assertEqual(client.resolve_calls, [])
        self.assertEqual(client.offer_calls, ["FID1"])
        self.assertEqual(result["offers_success"], 1)

    def test_legacy_expired_auth_failure_is_requeued_for_resume(self):
        run_id = self.staging(1)
        bootstrap = self.store.create_bootstrap(run_id, target_count=1, batch_size=1)
        product = self.store.products(bootstrap["bootstrap_run_id"])[0]
        self.store.persist_fid(
            bootstrap["bootstrap_run_id"], product["canonical_product_key"], "FID1",
            elapsed_seconds=0.1, attempts=1,
        )
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                """UPDATE qogita_bootstrap_products SET status='offers_permanent',
                          error_code='offers_http',
                          error_message='Qogita offers failed with HTTP 401'
                    WHERE bootstrap_run_id=?""",
                (bootstrap["bootstrap_run_id"],),
            )
        client = BootstrapFakeClient()
        result = run_qogita_bootstrap(
            bootstrap["bootstrap_run_id"], store=self.store, client=client,
            max_products=1, product_link_pacing=0, offers_pacing=0,
            sleep_func=lambda _: None,
        )
        self.assertEqual(client.resolve_calls, [])
        self.assertEqual(client.offer_calls, ["FID1"])
        self.assertEqual(result["offers_success"], 1)

    def test_failure_isolation_continues_with_next_product(self):
        run_id = self.staging(3)
        bootstrap = self.store.create_bootstrap(run_id, target_count=3, batch_size=3)
        products = self.store.products(bootstrap["bootstrap_run_id"])
        client = BootstrapFakeClient(fail_gtins={products[0]["gtin"]})
        result = run_qogita_bootstrap(
            bootstrap["bootstrap_run_id"], store=self.store, client=client,
            max_products=3, product_link_pacing=0, offers_pacing=0, sleep_func=lambda _: None,
        )
        self.assertEqual(result["fid_failed"], 1)
        self.assertEqual(result["offers_success"], 2)
        self.assertEqual(len(client.offer_calls), 2)

    def test_even_sample_and_queue_order_are_deterministic(self):
        run_id = self.staging(10)
        bootstrap = self.store.create_bootstrap(run_id, target_count=4, batch_size=2)
        rows = self.store.products(bootstrap["bootstrap_run_id"])
        self.assertEqual([row["sequence_no"] for row in rows], [1, 2, 3, 4])
        self.assertEqual(rows[0]["gtin"], valid_gtin(1))
        self.assertEqual(rows[-1]["gtin"], valid_gtin(10))
        self.assertEqual([row["sequence_no"] for row in self.store.next_batch(
            bootstrap["bootstrap_run_id"], limit=2,
        )], [1, 2])

    def test_sample_can_exclude_a_previous_bootstrap(self):
        run_id = self.staging(20)
        first = self.store.create_bootstrap(run_id, target_count=5, batch_size=2)
        second = self.store.create_bootstrap(
            run_id, target_count=10, batch_size=2,
            exclude_bootstrap_run_ids=(first["bootstrap_run_id"],),
        )
        first_keys = {row["canonical_product_key"] for row in self.store.products(first["bootstrap_run_id"])}
        second_keys = {row["canonical_product_key"] for row in self.store.products(second["bootstrap_run_id"])}
        self.assertEqual(len(second_keys), 10)
        self.assertFalse(first_keys & second_keys)

    def test_claim_lease_prevents_duplicates_and_expired_claim_is_recoverable(self):
        run_id = self.staging(2)
        bootstrap = self.store.create_bootstrap(run_id, target_count=2, batch_size=2)
        first = self.store.claim_batch(
            bootstrap["bootstrap_run_id"], worker_id="one", limit=1,
            lease_seconds=10, now="2026-08-26T10:00:00Z",
        )
        second = self.store.claim_batch(
            bootstrap["bootstrap_run_id"], worker_id="two", limit=1,
            lease_seconds=10, now="2026-08-26T10:00:01Z",
        )
        self.assertNotEqual(first[0]["canonical_product_key"], second[0]["canonical_product_key"])
        recovered = self.store.claim_batch(
            bootstrap["bootstrap_run_id"], worker_id="three", limit=1,
            lease_seconds=10, now="2026-08-26T10:00:11Z",
        )
        self.assertEqual(recovered[0]["canonical_product_key"], first[0]["canonical_product_key"])
        self.assertEqual(recovered[0]["claim_count"], 2)

    def test_two_workers_do_not_duplicate_processing_or_scenarios(self):
        run_id = self.staging(12)
        bootstrap = self.store.create_bootstrap(run_id, target_count=12, batch_size=4)
        clients = []
        def factory(_auth, _limiter):
            client = BootstrapFakeClient()
            clients.append(client)
            return client
        result = run_qogita_bootstrap_concurrent(
            bootstrap["bootstrap_run_id"], store=self.store, client_factory=factory,
            base_url="https://example.test", email="x", password="y", workers=2,
            max_products=12, checkpoint_every=4, product_link_pacing=0,
            offers_pacing=0, sleep_func=lambda _: None,
        )
        self.assertEqual(result["offers_success"], 12)
        self.assertEqual(result["claim_summary"]["claimed"], 0)
        self.assertEqual(sum(len(client.resolve_calls) for client in clients), 12)
        with sqlite3.connect(self.database) as connection:
            rows = connection.execute(
                "SELECT COUNT(*),COUNT(DISTINCT scenario_id) FROM supplier_catalog_scenarios WHERE run_id=?",
                (run_id,),
            ).fetchone()
        self.assertEqual(rows, (24, 24))

    def test_token_login_is_single_flight(self):
        auth = SharedQogitaAuth(
            base_url="https://example.test", email="x", password="y",
        )
        calls = []
        class Session:
            def post(self, *args, **kwargs):
                calls.append(1)
                time.sleep(0.02)
                return FakeResponse(200, {"access": "shared-token"})
        sessions = [Session(), Session()]
        with ThreadPoolExecutor(max_workers=2) as executor:
            tokens = list(executor.map(lambda session: auth.token(session)[0], sessions))
        self.assertEqual(tokens, ["shared-token", "shared-token"])
        self.assertEqual(len(calls), 1)

    def test_sqlite_lock_wait_is_measured_without_duplicate_claim(self):
        run_id = self.staging(1)
        bootstrap = self.store.create_bootstrap(run_id, target_count=1, batch_size=1)
        blocker = sqlite3.connect(self.database)
        blocker.execute("BEGIN IMMEDIATE")
        result = []
        thread = threading.Thread(target=lambda: result.extend(self.store.claim_batch(
            bootstrap["bootstrap_run_id"], worker_id="worker", limit=1,
        )))
        thread.start()
        time.sleep(0.05)
        blocker.rollback()
        blocker.close()
        thread.join(timeout=2)
        self.assertEqual(len(result), 1)
        self.assertGreater(self.store.sqlite_metrics["lock_wait_seconds"], 0)


if __name__ == "__main__":
    unittest.main()
