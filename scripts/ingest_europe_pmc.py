# scripts/ingest_europe_pmc.py
from datetime import date
import json
from pathlib import Path

from ingestion import EuropePMCClient, EuropePMCQuery

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
