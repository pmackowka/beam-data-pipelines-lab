# Beam Data Pipelines Lab

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
| 001 A–C | [`001_A_Imports_and_Setup.py`](001_A_Imports_and_Setup.py), [`001_B_ParDo_Square_Numbers.py`](001_B_ParDo_Square_Numbers.py), [`001_C_CombineFn_Average.py`](001_C_CombineFn_Average.py) | Pierwsze pipeline'y w Beam: przegląd importów, `ParDo`, własny `CombineFn` do liczenia średniej |
| 002 | [`002_Average_Calculation_and_BigQuery_Load.py`](002_Average_Calculation_and_BigQuery_Load.py) | Minimalny pipeline zapisujący zagregowany wynik do BigQuery przez Dataflow |

### Podstawowe transformacje
| # | Notebook | Temat |
|---|---|---|
| 003 | [`003_Structure_of_a_Beam_Pipeline_and_IO.ipynb`](003_Structure_of_a_Beam_Pipeline_and_IO.ipynb) | Anatomia pipeline'u i konektory I/O |
| 004 | [`004_Create_Transform.ipynb`](004_Create_Transform.ipynb) | Transformacja `Create` |
| 005 | [`005_Map_FlatMap_and_Filter.ipynb`](005_Map_FlatMap_and_Filter.ipynb) | `Map`, `FlatMap`, `Filter` |
| 006 | [`006_Branching_Pipelines_and_Word_Count.ipynb`](006_Branching_Pipelines_and_Word_Count.ipynb) | Rozgałęzianie pipeline'ów, klasyczny word count |
| 007 | [`007_ParDo_Transform.ipynb`](007_ParDo_Transform.ipynb) | `ParDo` i `DoFn` |
| 008 | [`008_Advanced_Combiner_of_Beam.ipynb`](008_Advanced_Combiner_of_Beam.ipynb) | Zaawansowane combinery |
| 009 | [`009_Create_Composite_Transforms.ipynb`](009_Create_Composite_Transforms.ipynb) | Budowanie złożonych transformacji |
| 010 | [`010_CoGroupBy_for_Joins.ipynb`](010_CoGroupBy_for_Joins.ipynb) | Łączenie PCollections przez `CoGroupByKey` |
| 011 | [`011_Beam_Transform_Methods_Reference.ipynb`](011_Beam_Transform_Methods_Reference.ipynb) | Notebook-ściąga zestawiająca obok siebie kluczowe metody Beam |

### Streaming z Pub/Sub
| # | Notebook | Temat |
|---|---|---|
| 012 A–C | [`012_A_Publish_to_PubSub_Streaming_Pipeline.ipynb`](012_A_Publish_to_PubSub_Streaming_Pipeline.ipynb), [`012_B_...`](012_B_Process_PubSub_Streaming_Pipeline_on_GCP.ipynb), [`012_C_...`](012_C_Subscribe_to_PubSub_Streaming_Pipeline_on_GCP.ipynb) | Wzorzec publish / process / subscribe dla pipeline'u strumieniowego Pub/Sub na GCP |

### Windowing i watermarki
| # | Notebook | Temat |
|---|---|---|
| 013 | [`013_Introduction_to_Windows_WindowInto.ipynb`](013_Introduction_to_Windows_WindowInto.ipynb) | Podstawy `beam.WindowInto()` |
| 014 | [`014_Implementing_FixedWindows_with_Timestamp.ipynb`](014_Implementing_FixedWindows_with_Timestamp.ipynb) | Fixed windows, dane z timestampem |
| 015 | [`015_Implementing_FixedWindows_without_Timestamp.ipynb`](015_Implementing_FixedWindows_without_Timestamp.ipynb) | Fixed windows, dane bez timestampu |
| 016 | [`016_Implementing_SlidingWindows_without_Timestamp.ipynb`](016_Implementing_SlidingWindows_without_Timestamp.ipynb) | Sliding windows |
| 017 | [`017_Implementing_SessionWindows_without_Timestamp.ipynb`](017_Implementing_SessionWindows_without_Timestamp.ipynb) | Session windows |
| 018 | [`018_Implementing_GlobalWindows_without_Timestamp.ipynb`](018_Implementing_GlobalWindows_without_Timestamp.ipynb) | Global windows |
| 019 | [`019_Watermarks.ipynb`](019_Watermarks.ipynb) | Watermarki i spóźnione dane |

### Projekty aplikacyjne
| # | Notebook | Temat |
|---|---|---|
| 020 | [`020_Mobile_Game_Analysis.ipynb`](020_Mobile_Game_Analysis.ipynb) | Analiza end-to-end zdarzeń bitewnych z gry mobilnej |
| 024 | [`024_Identify_Bank_Loan_Defaulters.ipynb`](024_Identify_Bank_Loan_Defaulters.ipynb) | Identyfikacja klientów banku niespłacających kredytów na podstawie danych o kartach i pożyczkach |

### Integracja z GCP
| # | Notebook | Temat |
|---|---|---|
| 021 | [`021_Deploy_Beam_Pipeline_on_Google_Cloud_Dataflow.ipynb`](021_Deploy_Beam_Pipeline_on_Google_Cloud_Dataflow.ipynb) | Wdrożenie pipeline'u na Dataflow |
| 022 | [`022_Write_to_BigQuery_Tables.ipynb`](022_Write_to_BigQuery_Tables.ipynb) | Zapis wyniku pipeline'u do BigQuery |

### Tematy zaawansowane
| # | Notebook | Temat |
|---|---|---|
| 023 | [`023_Side_Inputs_and_Outputs.ipynb`](023_Side_Inputs_and_Outputs.ipynb) | Side inputs i side outputs |
| 025 | [`025_Type_Hints_in_Beam.ipynb`](025_Type_Hints_in_Beam.ipynb) | Type hints w transformacjach Beam |

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
