import json
import sqlite3
from pathlib import Path

DB_PATH = Path('data/powershell-checks/europepmc.sqlite')
JSONL_PATH = Path('data/powershell-checks/sentence_events_for_sentiment.jsonl')

def safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('utf-8', 'replace').decode('utf-8', 'replace'))

def main() -> None:
    text_lookup = {}
    if JSONL_PATH.exists():
        with JSONL_PATH.open(encoding='utf-8') as fh:
            for line in fh:
                payload = json.loads(line)
                text_lookup[payload.get('sentence_id')] = payload.get('sentence_text')

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        '''
        SELECT sentence_id, product_a, product_b,
               narrative_type, narrative_subtype,
               narrative_invariant_reason
        FROM sentence_events
        WHERE narrative_type IS NOT NULL
          AND narrative_invariant_ok = 0
        ORDER BY created_at DESC
        LIMIT 20
        '''
    )

    for sentence_id, prod_a, prod_b, n_type, n_sub, reason in cursor:
        text = text_lookup.get(sentence_id, '<text not found>')
        safe_print('=' * 80)
        safe_print(f'Sentence ID: {sentence_id}')
        safe_print(f'Products: {prod_a} vs {prod_b}')
        safe_print(f'Narrative: {n_type}/{n_sub}')
        safe_print(f'Guardrail reason: {reason}')
        safe_print('Text:')
        safe_print(text or '')

    conn.close()

if __name__ == '__main__':
    main()
