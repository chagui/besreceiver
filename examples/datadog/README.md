## Purpose

Development setup for iterating on besreceiver with traces flowing to Datadog.
Uses `air` for hot-reload — the collector rebuilds automatically when Go or YAML files change.
Copy `.env.example` to `.env` and set `DD_API_KEY` and `DD_SITE`, then run `docker compose up`.
