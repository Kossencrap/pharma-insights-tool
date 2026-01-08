# Test coverage gaps

This file lists notable areas that current tests do not yet cover.

- The `which_doc.py` CLI still lacks automated tests; other Phase 2 scripts (aggregate metrics, sentence-event labeling) are now covered via dedicated CLI and pipeline tests.
- The full Europe PMC client path (network requests, cursor-based pagination, and error handling) is not mocked or validated; tests only verify parameter forwarding such as `resultType`.
- Several helper paths in `storage/sqlite_store.py` (e.g., update and delete operations) are not covered directly.
- Evidence display scripts—including sentence event labeling and interactive document lookups—are not tested.

## Wanneer deze gaten opgevuld moeten worden
- **Tijdens feature-ontwikkeling**: als je een van deze scripts of codepaden wijzigt, voeg meteen tests toe zodat de wijziging gedekt is voordat deze gemerged wordt.
- **Voor release of deployment**: zorg dat de belangrijkste gebruikersscenario's (bijv. aggregatie, co-mention queries, evidence-labeling) getest zijn voordat je een nieuwe versie uitrolt.
- **Voor grote refactors**: schrijf regressietests voor de relevante paden voordat je ze herstructureert, zodat je na de refactor kunt verifiëren dat gedrag gelijk blijft.
