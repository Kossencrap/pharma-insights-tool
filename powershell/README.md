# PowerShell functional checks

`functional_checks.ps1` biedt één commandorunbook om snel te controleren of de belangrijkste functionaliteiten van dit project werken.

## Gebruik

Voer het script vanuit de projectroot:

```powershell
pwsh ./powershell/functional_checks.ps1
```

Of, met Windows PowerShell:

```powershell
powershell ./powershell/functional_checks.ps1
```

Standaard acties:
- Draait `pytest` om de unit/integratietests te draaien.
- Voert een kleine Europe PMC-ingestie uit voor een voorbeeldproduct en slaat resultaten op in `data/powershell-checks/europepmc.sqlite`.
- Labelt co-mention-zinnen met `label_sentence_events.py`.
- Berekent wekelijkse/maandelijkse metrics met `aggregate_metrics.py`.
- Exporteert sentiment-ratio metrics met `export_sentiment_metrics.py`.
- Draait `query_comentions.py`, `which_doc.py` en `show_sentence_evidence.py` tegen die database als functionele check.
- Start optioneel de Streamlit dashboards voor evidence en metrics (indien geïnstalleerd).

### Handige vlaggen

- `-PythonExe` – kies een andere Python-binary, bijv. `python` i.p.v. `py`.
- `-DataRoot` – wijzig het pad waar de tijdelijke database wordt opgeslagen.
- `-Products` – pas de lijst met zoektermen aan (array), bijv. `-Products "aspirin","ibuprofen"`.
- `-SkipPytests` – sla de `pytest`-stap over.
- `-SkipNetworkSteps` – sla ingestie én downstream queries over (handig offline).
- `-SkipFunctionalQueries` – sla alleen de query-scripts over na ingestie.
- `-SkipStreamlit` – sla optionele Streamlit apps over.

Breid het script uit met extra controles zodra er nieuwe functionaliteit bijkomt.
