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
| 001 A–C | [`001_a_imports_and_setup.py`](001_a_imports_and_setup.py), [`001_b_pardo_square_numbers.py`](001_b_pardo_square_numbers.py), [`001_c_combine_fn_average.py`](001_c_combine_fn_average.py) | Pierwsze pipeline'y w Beam: przegląd importów, `ParDo`, własny `CombineFn` do liczenia średniej |
| 002 | [`002_average_calculation_and_bigquery_load.py`](002_average_calculation_and_bigquery_load.py) | Minimalny pipeline zapisujący zagregowany wynik do BigQuery przez Dataflow |

### Podstawowe transformacje
| # | Notebook | Temat |
|---|---|---|
| 003 | [`003_structure_of_a_beam_pipeline_and_io.ipynb`](003_structure_of_a_beam_pipeline_and_io.ipynb) | Anatomia pipeline'u i konektory I/O |
| 004 | [`004_create_transform.ipynb`](004_create_transform.ipynb) | Transformacja `Create` |
| 005 | [`005_map_flat_map_and_filter.ipynb`](005_map_flat_map_and_filter.ipynb) | `Map`, `FlatMap`, `Filter` |
| 006 | [`006_branching_pipelines_and_word_count.ipynb`](006_branching_pipelines_and_word_count.ipynb) | Rozgałęzianie pipeline'ów, klasyczny word count |
| 007 | [`007_pardo_transform.ipynb`](007_pardo_transform.ipynb) | `ParDo` i `DoFn` |
| 008 | [`008_advanced_combiner_of_beam.ipynb`](008_advanced_combiner_of_beam.ipynb) | Zaawansowane combinery |
| 009 | [`009_create_composite_transforms.ipynb`](009_create_composite_transforms.ipynb) | Budowanie złożonych transformacji |
| 010 | [`010_co_group_by_for_joins.ipynb`](010_co_group_by_for_joins.ipynb) | Łączenie PCollections przez `CoGroupByKey` |
| 011 | [`011_beam_transform_methods_reference.ipynb`](011_beam_transform_methods_reference.ipynb) | Notebook-ściąga zestawiająca obok siebie kluczowe metody Beam |

### Streaming z Pub/Sub
| # | Notebook | Temat |
|---|---|---|
| 012 A–C | [`012_a_publish_to_pubsub_streaming_pipeline.ipynb`](012_a_publish_to_pubsub_streaming_pipeline.ipynb), [`012_b_...`](012_b_process_pubsub_streaming_pipeline_on_gcp.ipynb), [`012_c_...`](012_c_subscribe_to_pubsub_streaming_pipeline_on_gcp.ipynb) | Wzorzec publish / process / subscribe dla pipeline'u strumieniowego Pub/Sub na GCP |

### Windowing i watermarki
| # | Notebook | Temat |
|---|---|---|
| 013 | [`013_introduction_to_windows_window_into.ipynb`](013_introduction_to_windows_window_into.ipynb) | Podstawy `beam.WindowInto()` |
| 014 | [`014_implementing_fixed_windows_with_timestamp.ipynb`](014_implementing_fixed_windows_with_timestamp.ipynb) | Fixed windows, dane z timestampem |
| 015 | [`015_implementing_fixed_windows_without_timestamp.ipynb`](015_implementing_fixed_windows_without_timestamp.ipynb) | Fixed windows, dane bez timestampu |
| 016 | [`016_implementing_sliding_windows_without_timestamp.ipynb`](016_implementing_sliding_windows_without_timestamp.ipynb) | Sliding windows |
| 017 | [`017_implementing_session_windows_without_timestamp.ipynb`](017_implementing_session_windows_without_timestamp.ipynb) | Session windows |
| 018 | [`018_implementing_global_windows_without_timestamp.ipynb`](018_implementing_global_windows_without_timestamp.ipynb) | Global windows |
| 019 | [`019_watermarks.ipynb`](019_watermarks.ipynb) | Watermarki i spóźnione dane |

### Projekty aplikacyjne
| # | Notebook | Temat |
|---|---|---|
| 020 | [`020_mobile_game_analysis.ipynb`](020_mobile_game_analysis.ipynb) | Analiza end-to-end zdarzeń bitewnych z gry mobilnej |
| 024 | [`024_identify_bank_loan_defaulters.ipynb`](024_identify_bank_loan_defaulters.ipynb) | Identyfikacja klientów banku niespłacających kredytów na podstawie danych o kartach i pożyczkach |

### Integracja z GCP
| # | Notebook | Temat |
|---|---|---|
| 021 | [`021_deploy_beam_pipeline_on_google_cloud_dataflow.ipynb`](021_deploy_beam_pipeline_on_google_cloud_dataflow.ipynb) | Wdrożenie pipeline'u na Dataflow |
| 022 | [`022_write_to_bigquery_tables.ipynb`](022_write_to_bigquery_tables.ipynb) | Zapis wyniku pipeline'u do BigQuery |

### Tematy zaawansowane
| # | Notebook | Temat |
|---|---|---|
| 023 | [`023_side_inputs_and_outputs.ipynb`](023_side_inputs_and_outputs.ipynb) | Side inputs i side outputs |
| 025 | [`025_type_hints_in_beam.ipynb`](025_type_hints_in_beam.ipynb) | Type hints w transformacjach Beam |

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
