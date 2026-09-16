# Beam Data Pipelines Lab

Hands-on practice repository for Apache Beam's Python SDK, covering batch and streaming data processing — from core transforms to windowing, Pub/Sub streaming, and deployment on Google Cloud Dataflow with BigQuery as a sink.

Most notebooks were built and run in Google Colab (`!pip install apache-beam`, `/content/...` paths), then committed here as a running log of the material as I went through it.

## Tech Stack

- **Apache Beam** (Python SDK) — batch & streaming pipelines
- **Google Cloud Dataflow** — managed runner
- **Google Cloud Pub/Sub** — streaming source/sink
- **Google BigQuery** — pipeline output
- **pandas / numpy** — data prep in a few notebooks
- **Google Colab** — primary runtime for most notebooks

## Repository Structure

Files are numbered chronologically (`001` → `025`) in the order the topics were worked through. The table below groups them by topic for easier browsing.

### Getting Started
| # | Notebook / Script | Topic |
|---|---|---|
| 001 | [`001_Getting_Started/`](001_Getting_Started) | First Beam pipelines: imports overview, `ParDo`, a custom `CombineFn` for averages |
| 002 | [`002_Average_Calculation_and_BigQuery_Load/`](002_Average_Calculation_and_BigQuery_Load) | Minimal pipeline writing an aggregated result to BigQuery via Dataflow |

### Core Transforms
| # | Notebook | Topic |
|---|---|---|
| 003 | [`003_Structure_of_a_Beam_Pipeline_and_IO.ipynb`](003_Structure_of_a_Beam_Pipeline_and_IO.ipynb) | Pipeline anatomy and I/O connectors |
| 004 | [`004_Create_Transform.ipynb`](004_Create_Transform.ipynb) | `Create` transform |
| 005 | [`005_Map_FlatMap_and_Filter.ipynb`](005_Map_FlatMap_and_Filter.ipynb) | `Map`, `FlatMap`, `Filter` |
| 006 | [`006_Branching_Pipelines_and_Word_Count.ipynb`](006_Branching_Pipelines_and_Word_Count.ipynb) | Branching pipelines, classic word count |
| 007 | [`007_ParDo_Transform.ipynb`](007_ParDo_Transform.ipynb) | `ParDo` and `DoFn` |
| 008 | [`008_Advanced_Combiner_of_Beam.ipynb`](008_Advanced_Combiner_of_Beam.ipynb) | Advanced combiners |
| 009 | [`009_Create_Composite_Transforms.ipynb`](009_Create_Composite_Transforms.ipynb) | Building composite transforms |
| 010 | [`010_CoGroupBy_for_Joins.ipynb`](010_CoGroupBy_for_Joins.ipynb) | Joining PCollections with `CoGroupByKey` |
| 011 | [`011_Beam_Transform_Methods_Reference.ipynb`](011_Beam_Transform_Methods_Reference.ipynb) | Reference notebook covering core Beam methods side by side |

### Streaming with Pub/Sub
| # | Notebook | Topic |
|---|---|---|
| 012 A–C | [`012_A_Publish_Run_PubSub_Streaming_Pipeline.ipynb`](012_A_Publish_Run_PubSub_Streaming_Pipeline.ipynb), [`012_B_...`](012_B_Process_Run_PubSub_Streaming_Pipeline_on_GCP.ipynb), [`012_C_...`](012_C_Subscribe_Run_PubSub_Streaming_Pipeline_on_GCP.ipynb) | Publish / process / subscribe pattern for a Pub/Sub streaming pipeline on GCP |

### Windowing & Watermarks
| # | Notebook | Topic |
|---|---|---|
| 013 | [`013_Introduction_to_Windows_WindowInto.ipynb`](013_Introduction_to_Windows_WindowInto.ipynb) | `beam.WindowInto()` basics |
| 014 | [`014_Implementing_FixedWindows_with_Timestamp.ipynb`](014_Implementing_FixedWindows_with_Timestamp.ipynb) | Fixed windows, timestamped data |
| 015 | [`015_Implementing_FixedWindows_without_Timestamp.ipynb`](015_Implementing_FixedWindows_without_Timestamp.ipynb) | Fixed windows, no timestamp |
| 016 | [`016_Implementing_SlidingWindows_without_Timestamp.ipynb`](016_Implementing_SlidingWindows_without_Timestamp.ipynb) | Sliding windows |
| 017 | [`017_Implementing_SessionWindows_without_Timestamp.ipynb`](017_Implementing_SessionWindows_without_Timestamp.ipynb) | Session windows |
| 018 | [`018_Implementing_GlobalWindows_without_Timestamp.ipynb`](018_Implementing_GlobalWindows_without_Timestamp.ipynb) | Global windows |
| 019 | [`019_Watermarks.ipynb`](019_Watermarks.ipynb) | Watermarks and late data |

### Applied Projects
| # | Notebook | Topic |
|---|---|---|
| 020 | [`020_Mobile_Game_Analysis.ipynb`](020_Mobile_Game_Analysis.ipynb) | End-to-end analysis of mobile game battle events |
| 024 | [`024_Identify_Banks_Defaulter_Customers.ipynb`](024_Identify_Banks_Defaulter_Customers.ipynb) | Identifying bank loan defaulters from card + loan data |

### GCP Integration
| # | Notebook | Topic |
|---|---|---|
| 021 | [`021_Deploy_Beam_Pipeline_on_Google_Cloud_Dataflow.ipynb`](021_Deploy_Beam_Pipeline_on_Google_Cloud_Dataflow.ipynb) | Deploying a pipeline on Dataflow |
| 022 | [`022_Write_to_BigQuery_Tables.ipynb`](022_Write_to_BigQuery_Tables.ipynb) | Writing pipeline output to BigQuery |

### Advanced Topics
| # | Notebook | Topic |
|---|---|---|
| 023 | [`023_Side_Inputs_and_Outputs.ipynb`](023_Side_Inputs_and_Outputs.ipynb) | Side inputs and side outputs |
| 025 | [`025_Type_Hints_in_Beam.ipynb`](025_Type_Hints_in_Beam.ipynb) | Type hints in Beam transforms |

## Running the Notebooks

Most notebooks install Beam inline and expect input files to already exist in the runtime (Colab's `/content/`):

```bash
pip install apache-beam[gcp]
```

To run locally instead of Colab, adjust the file paths at the top of each notebook and provide your own:

- GCP project ID and service account credentials (`GOOGLE_APPLICATION_CREDENTIALS`)
- Pub/Sub topics/subscriptions (for the streaming notebooks)
- GCS bucket for staging/temp locations (Dataflow notebooks)
- Source CSVs referenced by the applied-project notebooks (not included in this repo)

All project IDs, buckets, and credentials shown in the notebooks are placeholders — replace them with your own before running.

## Notes

This is a personal learning repository, not a production library — code favors clarity over reusability, and some notebooks intentionally re-implement the same pipeline a few different ways to compare approaches (e.g. custom `CombineFn` vs. built-in `Mean.Globally()`).
