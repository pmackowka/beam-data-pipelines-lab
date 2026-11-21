# beam-data-pipelines-lab

[![License: MIT](https://img.shields.io/github/license/pmackowka/beam-data-pipelines-lab)](LICENSE)

Praktyczne repozytorium nauki Apache Beam (Python SDK) — przetwarzanie danych wsadowe i strumieniowe: od podstawowych transformacji, przez windowing, streaming z Pub/Sub, po wdrożenie na Google Cloud Dataflow z BigQuery jako sinkiem.

Większość notebooków powstała i była uruchamiana w Google Colab (`!pip install apache-beam`, ścieżki `/content/...`), a potem trafiła tutaj jako zapis materiału w kolejności, w jakiej był przerabiany.

## Stack technologiczny

- **Apache Beam** (Python SDK) — pipeline'y wsadowe i strumieniowe
- **Google Cloud Dataflow** — managed runner
- **Google Cloud Pub/Sub** — źródło/sink dla streamingu
- **Google BigQuery** — output pipeline'ów
- **pandas / numpy** — przygotowanie danych w kilku notebookach
- **Google Colab** — środowisko uruchomieniowe dla większości notebooków

## Struktura repozytorium

Pliki są numerowane chronologicznie (`001` → `025`) w kolejności, w jakiej przerabiane były tematy. Poniższa tabela grupuje je tematycznie dla łatwiejszego przeglądania.

### Start
| # | Skrypt | Temat |
|---|---|---|
| 001 A–C | [`001-a-imports-and-setup.py`](001-a-imports-and-setup.py), [`001-b-pardo-square-numbers.py`](001-b-pardo-square-numbers.py), [`001-c-combine-fn-average.py`](001-c-combine-fn-average.py) | Pierwsze pipeline'y w Beam: przegląd importów, `ParDo`, własny `CombineFn` do liczenia średniej |
| 002 | [`002-average-calculation-and-bigquery-load.py`](002-average-calculation-and-bigquery-load.py) | Minimalny pipeline zapisujący zagregowany wynik do BigQuery przez Dataflow |

### Podstawowe transformacje
| # | Notebook | Temat |
|---|---|---|
| 003 | [`003-structure-of-a-beam-pipeline-and-io.ipynb`](003-structure-of-a-beam-pipeline-and-io.ipynb) | Anatomia pipeline'u i konektory I/O |
| 004 | [`004-create-transform.ipynb`](004-create-transform.ipynb) | Transformacja `Create` |
| 005 | [`005-map-flat-map-and-filter.ipynb`](005-map-flat-map-and-filter.ipynb) | `Map`, `FlatMap`, `Filter` |
| 006 | [`006-branching-pipelines-and-word-count.ipynb`](006-branching-pipelines-and-word-count.ipynb) | Rozgałęzianie pipeline'ów, klasyczny word count |
| 007 | [`007-pardo-transform.ipynb`](007-pardo-transform.ipynb) | `ParDo` i `DoFn` |
| 008 | [`008-advanced-combiner-of-beam.ipynb`](008-advanced-combiner-of-beam.ipynb) | Zaawansowane combinery |
| 009 | [`009-create-composite-transforms.ipynb`](009-create-composite-transforms.ipynb) | Budowanie złożonych transformacji |
| 010 | [`010-co-group-by-for-joins.ipynb`](010-co-group-by-for-joins.ipynb) | Łączenie PCollections przez `CoGroupByKey` |
| 011 | [`011-beam-transform-methods-reference.ipynb`](011-beam-transform-methods-reference.ipynb) | Notebook-ściąga zestawiająca obok siebie kluczowe metody Beam |

### Streaming z Pub/Sub
| # | Notebook | Temat |
|---|---|---|
| 012 A–C | [`012-a-publish-to-pubsub-streaming-pipeline.ipynb`](012-a-publish-to-pubsub-streaming-pipeline.ipynb), [`012-b-...`](012-b-process-pubsub-streaming-pipeline-on-gcp.ipynb), [`012-c-...`](012-c-subscribe-to-pubsub-streaming-pipeline-on-gcp.ipynb) | Wzorzec publish / process / subscribe dla pipeline'u strumieniowego Pub/Sub na GCP |

### Windowing i watermarki
| # | Notebook | Temat |
|---|---|---|
| 013 | [`013-introduction-to-windows-window-into.ipynb`](013-introduction-to-windows-window-into.ipynb) | Podstawy `beam.WindowInto()` |
| 014 | [`014-implementing-fixed-windows-with-timestamp.ipynb`](014-implementing-fixed-windows-with-timestamp.ipynb) | Fixed windows, dane z timestampem |
| 015 | [`015-implementing-fixed-windows-without-timestamp.ipynb`](015-implementing-fixed-windows-without-timestamp.ipynb) | Fixed windows, dane bez timestampu |
| 016 | [`016-implementing-sliding-windows-without-timestamp.ipynb`](016-implementing-sliding-windows-without-timestamp.ipynb) | Sliding windows |
| 017 | [`017-implementing-session-windows-without-timestamp.ipynb`](017-implementing-session-windows-without-timestamp.ipynb) | Session windows |
| 018 | [`018-implementing-global-windows-without-timestamp.ipynb`](018-implementing-global-windows-without-timestamp.ipynb) | Global windows |
| 019 | [`019-watermarks.ipynb`](019-watermarks.ipynb) | Watermarki i spóźnione dane |

### Projekty aplikacyjne
| # | Notebook | Temat |
|---|---|---|
| 020 | [`020-mobile-game-analysis.ipynb`](020-mobile-game-analysis.ipynb) | Analiza end-to-end zdarzeń bitewnych z gry mobilnej |
| 024 | [`024-identify-bank-loan-defaulters.ipynb`](024-identify-bank-loan-defaulters.ipynb) | Identyfikacja klientów banku niespłacających kredytów na podstawie danych o kartach i pożyczkach |

### Integracja z GCP
| # | Notebook | Temat |
|---|---|---|
| 021 | [`021-deploy-beam-pipeline-on-google-cloud-dataflow.ipynb`](021-deploy-beam-pipeline-on-google-cloud-dataflow.ipynb) | Wdrożenie pipeline'u na Dataflow |
| 022 | [`022-write-to-bigquery-tables.ipynb`](022-write-to-bigquery-tables.ipynb) | Zapis wyniku pipeline'u do BigQuery |

### Tematy zaawansowane
| # | Notebook | Temat |
|---|---|---|
| 023 | [`023-side-inputs-and-outputs.ipynb`](023-side-inputs-and-outputs.ipynb) | Side inputs i side outputs |
| 025 | [`025-type-hints-in-beam.ipynb`](025-type-hints-in-beam.ipynb) | Type hints w transformacjach Beam |

## Uruchamianie notebooków

Większość notebooków instaluje Beam bezpośrednio w komórce i zakłada, że pliki wejściowe już istnieją w środowisku (Colab, `/content/`):

```bash
pip install apache-beam[gcp]
```

Żeby uruchomić lokalnie zamiast w Colabie, popraw ścieżki plików na górze każdego notebooka i podaj własne:

- ID projektu GCP i dane uwierzytelniające service account (`GOOGLE_APPLICATION_CREDENTIALS`)
- topiki/subskrypcje Pub/Sub (dla notebooków strumieniowych)
- bucket GCS pod lokalizacje staging/temp (notebooki Dataflow)
- pliki CSV, do których odwołują się notebooki z projektami aplikacyjnymi (nie są dołączone do repo)

Wszystkie ID projektów, buckety i dane uwierzytelniające widoczne w notebookach to placeholdery — przed uruchomieniem podmień je na własne.

## Uwagi

To repozytorium nauki, nie biblioteka produkcyjna — kod stawia na czytelność, nie na reużywalność, a część notebooków celowo implementuje ten sam pipeline na kilka sposobów, żeby porównać podejścia (np. własny `CombineFn` vs. wbudowany `Mean.Globally()`).
