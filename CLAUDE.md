# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A personal Apache Beam (Python SDK) learning log: numbered, flat Jupyter notebooks and scripts (`001`–`025`) covering batch/streaming transforms, windowing, Pub/Sub streaming, and deployment to Google Cloud Dataflow/BigQuery. See `README.md` (Polish) for the full topic-by-topic table of contents.

There is no build, lint, or test tooling in this repo — it's not a package. Most notebooks are meant to run in Google Colab: they `!pip install apache-beam` inline and read input files from `/content/...`. To run one locally instead, install with `pip install apache-beam[gcp]` and adjust the file paths at the top of the notebook.

## Conventions to preserve

- **File naming**: flat files at repo root, `NNN_Descriptive_English_Title.ipynb` (or `.py`), underscores only, Title_Case, no spaces/parentheses. Numbers are chronological, not thematic — don't renumber existing files when adding a new one, just continue from the highest existing number. When one topic spans multiple files (e.g. a publish/process/subscribe trio, or a multi-step intro), reuse the same number with an `_A_`/`_B_`/`_C_` suffix instead of creating a subfolder (see `001_A/B/C` and `012_A/B/C`).
- **Language split**: file/folder names, identifiers, and README prose are English; in-notebook comments and markdown explanations are Polish. Keep this split when adding or editing content — don't translate existing Polish code comments to English or vice versa.
- **No real credentials or infra identifiers**: all GCP project IDs, bucket names, topic/subscription names, and service-account paths in this repo are placeholders (`your-gcp-project`, `your-project-bucket`, `service-account.json`, etc.). A previous cleanup pass scrubbed a real former-employer GCP project ID and username that had leaked into several streaming/windowing notebooks (012, 014–020, 022) — never reintroduce a real project ID, bucket, credential path, or username when editing these or adding new cloud-integration notebooks.
- **README is in Polish** — this was a deliberate choice (initially drafted in English, translated to Polish on request). Keep new README sections in Polish unless told otherwise.
