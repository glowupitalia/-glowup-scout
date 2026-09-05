import unittest

from storage_retention import (
    RetentionAbort,
    RetentionCycleLimits,
    run_retention_cycle,
)


def measured(state="PRESSURE"):
    return {
        "reliable": True,
        "watermark": {"reliable": True, "state": state, "version": "v1"},
    }


class FakeCycleExecutor:
    def __init__(self, candidates, *, outcome="APPLIED", fail=None):
        self.metrics_provider = lambda: measured()
        self.candidates = list(candidates)
        self.outcome = outcome
        self.fail = fail
        self.build_calls = []
        self.apply_calls = []
        self.audit = []

    def build_plan(self, state, job_ids=None):
        self.build_calls.append((state, tuple(job_ids or ())))
        rows = [row for row in self.candidates if not job_ids or row["job_id"] in job_ids]
        return {
            "watermark_state": state,
            "candidates": rows,
            "plan_sha256": f"plan-{len(self.build_calls)}",
        }

    def preview(self, plan):
        return {"mode": "dry-run", "writes_performed": 0, "results": plan["candidates"]}

    def apply_plan(self, plan, **kwargs):
        self.apply_calls.append(plan)
        if self.fail:
            raise RetentionAbort(self.fail)
        candidate = plan["candidates"][0]
        status = self.outcome
        if status == "APPLIED":
            self.candidates = [
                row for row in self.candidates if row["job_id"] != candidate["job_id"]
            ]
        return {"results": [{
            "job_id": candidate["job_id"], "status": status,
            "logical_bytes_removed": candidate["manifest"]["logical_bytes_remove"],
            "audit_persisted": True,
        }]}

    def _audit(self, outcome, **details):
        self.audit.append((outcome, details))
        return True


def candidate(job_id, logical=100):
    return {"job_id": job_id, "manifest": {"logical_bytes_remove": logical}}


class RetentionCycleTests(unittest.TestCase):
    def test_automation_is_disabled_by_default(self):
        executor = FakeCycleExecutor([candidate("a")])
        result = run_retention_cycle(executor=executor)
        self.assertEqual(result["status"], "RETENTION_AUTOMATION_DISABLED")
        self.assertFalse(result["automatic_execution_configured"])
        self.assertEqual(executor.apply_calls, [])

    def test_dry_run_builds_preview_without_apply(self):
        executor = FakeCycleExecutor([candidate("a")])
        result = run_retention_cycle(executor=executor, enabled=True)
        self.assertEqual(result["status"], "DRY_RUN")
        self.assertEqual(result["watermark"], "PRESSURE")
        self.assertEqual(executor.apply_calls, [])

    def test_max_jobs_stops_after_first_verified_job(self):
        executor = FakeCycleExecutor([candidate("a"), candidate("b")])
        result = run_retention_cycle(
            executor=executor, enabled=True, apply=True,
            limits=RetentionCycleLimits(
                max_jobs_per_retention_cycle=1,
                max_logical_reclaim_per_cycle=10_000,
            ),
        )
        self.assertEqual(result["status"], "RETENTION_CYCLE_LIMIT_REACHED")
        self.assertEqual(result["limit"], "max_jobs_per_retention_cycle")
        self.assertEqual(result["jobs_applied"], 1)
        self.assertEqual(len(executor.apply_calls), 1)

    def test_max_bytes_stops_before_candidate(self):
        executor = FakeCycleExecutor([candidate("a", logical=101)])
        result = run_retention_cycle(
            executor=executor, enabled=True, apply=True,
            limits=RetentionCycleLimits(
                max_jobs_per_retention_cycle=5,
                max_logical_reclaim_per_cycle=100,
            ),
        )
        self.assertEqual(result["status"], "RETENTION_CYCLE_LIMIT_REACHED")
        self.assertEqual(result["limit"], "max_logical_reclaim_per_cycle")
        self.assertEqual(executor.apply_calls, [])

    def test_first_verification_error_stops_cycle(self):
        executor = FakeCycleExecutor(
            [candidate("a"), candidate("b")], fail="post_state_mismatch",
        )
        result = run_retention_cycle(executor=executor, enabled=True, apply=True)
        self.assertEqual(result["status"], "RETENTION_ABORT_VERIFICATION")
        self.assertIn("post_state_mismatch", result["reason"])
        self.assertEqual(len(executor.apply_calls), 1)

    def test_storage_is_remeasured_and_plan_regenerated_between_jobs(self):
        states = iter(("PRESSURE", "PRESSURE", "NORMAL", "NORMAL"))
        executor = FakeCycleExecutor([candidate("a"), candidate("b")])

        def changing_metrics():
            try:
                return measured(next(states))
            except StopIteration:
                return measured("NORMAL")

        result = run_retention_cycle(
            executor=executor, enabled=True, apply=True,
            metrics_provider=changing_metrics,
            limits=RetentionCycleLimits(
                max_jobs_per_retention_cycle=5,
                max_logical_reclaim_per_cycle=10_000,
            ),
        )
        self.assertEqual(result["status"], "STORAGE_PRESSURE_RESOLVED")
        self.assertEqual(result["jobs_applied"], 1)
        self.assertEqual(len(executor.apply_calls), 1)
        self.assertGreaterEqual(len(executor.build_calls), 2)

    def test_noop_consumes_no_cycle_budget_and_stops(self):
        executor = FakeCycleExecutor([candidate("already-final")], outcome="NOOP_ALREADY_APPLIED")
        result = run_retention_cycle(executor=executor, enabled=True, apply=True)
        self.assertEqual(result["status"], "RETENTION_NOOP")
        self.assertEqual(result["jobs_applied"], 0)
        self.assertEqual(result["logical_reclaim_applied"], 0)

    def test_unsupported_critical_state_never_invents_policy(self):
        executor = FakeCycleExecutor([candidate("a")])
        result = run_retention_cycle(
            executor=executor, enabled=True, apply=True,
            metrics_provider=lambda: measured("CRITICAL"),
        )
        self.assertEqual(result["status"], "NO_AUTOMATIC_POLICY_FOR_WATERMARK")
        self.assertEqual(executor.apply_calls, [])


if __name__ == "__main__":
    unittest.main()
