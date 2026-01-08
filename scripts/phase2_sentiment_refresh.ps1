# PowerShell helper to refresh sentence_events sentiment and sentiment metrics.

Set-Location "$PSScriptRoot\.."
$env:PYTHONPATH = (Get-Location).Path

py -m scripts.label_sentence_events `
  --db data\europepmc.sqlite `
  --limit 7500 `
  --since-publication 2022-01-01

py -m scripts.export_sentence_events_jsonl `
  --db data\europepmc.sqlite `
  --output data\processed\sentence_events_for_sentiment.jsonl

py -m scripts.label_sentence_sentiment `
  --input data\processed\sentence_events_for_sentiment.jsonl `
  --db data\europepmc.sqlite

py -m scripts.export_sentiment_metrics `
  --db data\europepmc.sqlite `
  --outdir data\artifacts\phase1\metrics
