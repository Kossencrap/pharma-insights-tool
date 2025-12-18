# Test coverage gaps

This file lists notable areas that current tests do not yet cover.

- Command-line scripts such as `aggregate_metrics.py`, `query_comentions.py`, `label_sentence_events.py`, `show_sentence_evidence.py`, and `which_doc.py` are not exercised by automated tests, so their logic is still unvalidated by the suite.
- The full Europe PMC client path (network requests, cursor-based pagination, and error handling) is not mocked or validated; tests only verify parameter forwarding such as `resultType`.
- Several helper paths in `storage/sqlite_store.py` (e.g., update and delete operations) are not covered directly.
- Evidence display scripts—including sentence event labeling and interactive document lookups—are not tested.
