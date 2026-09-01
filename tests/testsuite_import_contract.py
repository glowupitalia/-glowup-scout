import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"


class CleanProcessImportContractTests(unittest.TestCase):
    def _environment(self, root: Path) -> dict[str, str]:
        return {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "DISCOVERY_CHECKPOINT_ROOT": str(root / "checkpoints"),
            "DISCOVERY_INCREMENTAL_DATABASE": str(root / "incremental.sqlite3"),
            "DISCOVERY_JOB_DATABASE": str(root / "jobs.sqlite3"),
            "DISCOVERY_ROTATION_DATABASE": str(root / "rotation.sqlite3"),
            "SCOUT_SUPPLIER_LOCK_DIR": str(root / "locks"),
        }

    def _run_clean(self, source: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            return subprocess.run(
                [str(PROJECT_PYTHON), "-c", source],
                cwd=PROJECT_ROOT,
                env=self._environment(root),
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )

    def test_project_venv_cleanly_imports_app_and_authoritative_modes(self):
        result = self._run_clean("""
import json
import sys
import app_glowup
import discovery_category_modes as modes
import discovery_taxonomy as taxonomy
payload = {
    "executable": sys.executable,
    "taxonomy_file": taxonomy.__file__,
    "modes_file": modes.__file__,
    "modes": [taxonomy.MODE_ALL, taxonomy.MODE_ONLY_BEAUTY, taxonomy.MODE_MANUAL],
}
assert payload["modes"] == ["all_categories", "only_beauty", "manual_selection"]
assert payload["modes"] == [modes.MODE_ALL, modes.MODE_ONLY_BEAUTY, modes.MODE_MANUAL]
print(json.dumps(payload))
""")
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(Path(payload["executable"]), PROJECT_PYTHON)
        self.assertEqual(
            Path(payload["taxonomy_file"]), PROJECT_ROOT / "discovery_taxonomy.py"
        )
        self.assertEqual(
            Path(payload["modes_file"]), PROJECT_ROOT / "discovery_category_modes.py"
        )

    def test_streamlit_partial_reload_does_not_import_modes_from_stale_taxonomy(self):
        result = self._run_clean("""
import discovery_taxonomy as stale_taxonomy
for name in ("MODE_ALL", "MODE_ONLY_BEAUTY", "MODE_MANUAL"):
    delattr(stale_taxonomy, name)
assert not hasattr(stale_taxonomy, "MODE_MANUAL")
import app_glowup
import discovery_incremental_runner
from discovery_category_modes import MODE_ALL, MODE_ONLY_BEAUTY, MODE_MANUAL
assert (MODE_ALL, MODE_ONLY_BEAUTY, MODE_MANUAL) == (
    "all_categories", "only_beauty", "manual_selection"
)
""")
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
