# scripts/ingest_europe_pmc.py
"""Run a simple Europe PMC ingestion for one product.

This script is intended to work when executed directly (``python scripts/ingest_europe_pmc.py``)
without requiring an editable install. We therefore ensure the repository root is on the
``PYTHONPATH`` before importing from ``src``.
"""

from datetime import date
import json
from pathlib import Path
import sys

# Make ``src`` importable when the repo has not been installed in editable mode.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.ingestion.europe_pmc_client import (  # noqa: E402
    EuropePMCClient,
    EuropePMCQuery,
)

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    client = EuropePMCClient(polite_delay_s=0.1)

    # 1️⃣ Choose ONE product
    product_names = ["dupilumab", "Dupixent"]

    # 2️⃣ Build query
    query_str = client.build_drug_query(
        product_names=product_names,
        require_abstract=True,
        from_date=date(2022, 1, 1),
    )

    query = EuropePMCQuery(query=query_str, page_size=50)

    # 3️⃣ Run search
    results = list(client.search(query, max_records=50))

    # 4️⃣ Persist raw + normalized
    raw_path = RAW_DIR / "dupilumab_raw.json"
    norm_path = PROCESSED_DIR / "dupilumab_documents.jsonl"

    with raw_path.open("w", encoding="utf-8") as f:
        json.dump([r.raw for r in results], f, indent=2)

    with norm_path.open("w", encoding="utf-8") as f:
        for r in results:
            f.write(r.model_dump_json() + "\n")

    print(f"Ingested {len(results)} documents")


if __name__ == "__main__":
    main()
