# JSON to CSV Pipeline

A production-ready Python pipeline that converts **any JSON file** to CSV format with **intelligent nested data handling**.

**No configuration needed** - just run it and choose how to handle nested arrays!

## Features

- **Works with any JSON structure** - flat, nested objects, arrays of objects
- **3 Conversion Modes** - choose how to handle nested arrays:
  - **FLAT** - Arrays as JSON strings (simple, single file)
  - **EXPLODE** - Full denormalization (one row per nested item)
  - **RELATIONAL** - Separate linked CSVs (normalized, like database tables)
- **Interactive Preview** - see your JSON structure and sample output before converting
- **Large file support** - handles files over 500MB with streaming
- **Fast processing** - uses Polars for 10x faster deduplication
- **Progress bars** - visual feedback for large files
- **Logging** - all operations logged for debugging

## Quick Start

### 1. Install

```bash
git clone https://github.com/nkratastr/python-json-to-csv-pipeline.git
cd python-json-to-csv-pipeline

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

pip install -r requirements.txt
```

### 2. Convert JSON to CSV

**Interactive Mode (Recommended)**
```bash
python run_pipeline.py
# Enter your file path, then choose conversion mode
```

**Direct Mode**
```bash
# Skip interactive - use specific mode directly
python run_pipeline.py data/input/employees.json --mode 1  # FLAT
python run_pipeline.py data/input/employees.json --mode 2  # EXPLODE
python run_pipeline.py data/input/employees.json --mode 3  # RELATIONAL
```

**Python Code**
```python
from src.pipeline import JSONToCSVPipeline

pipeline = JSONToCSVPipeline("employees.json")

# Interactive - shows structure and lets you choose
output_files = pipeline.run_interactive()

# Or direct mode
output_files = pipeline.run_with_mode(mode=3)  # RELATIONAL
```

## Conversion Modes Explained

### Mode 1: FLAT
Arrays of objects are stored as JSON strings. Best for simple exports.

**Input:**
```json
{"id": "E001", "name": "John", "projects": [{"projectId": "P1"}, {"projectId": "P2"}]}
```

**Output (1 CSV, 1 row):**
| id | name | projects |
|----|------|----------|
| E001 | John | [{"projectId": "P1"}, {"projectId": "P2"}] |

### Mode 2: EXPLODE
Each nested item becomes a separate row. Parent data is duplicated.

**Output (1 CSV, 2 rows):**
| id | name | projects.projectId |
|----|------|-------------------|
| E001 | John | P1 |
| E001 | John | P2 |

### Mode 3: RELATIONAL
Separate linked CSV files, like database tables. No data duplication.

**Output (2 CSVs):**

`employees.csv`:
| id | name |
|----|------|
| E001 | John |

`projects.csv`:
| employees_id | projectId |
|--------------|-----------|
| E001 | P1 |
| E001 | P2 |

## Interactive Preview

When you run in interactive mode, you'll see a structure analysis:

```
============================================================
  JSON STRUCTURE ANALYSIS
============================================================
  File: employees.json
  Records: 28,212
  Nesting Depth: 4

  DETECTED STRUCTURE
  ----------------------------------------
  └── employee (object)
      ├── id (string)
      ├── name (string)
      └── projects (array of objects)
          ├── projectId (string)
          └── tasks (array of objects)
              ├── taskId (string)
              └── title (string)

  ############################################################
  NESTED ARRAYS DETECTED
  ############################################################
  Arrays found: projects, tasks

  [1] FLAT - Single CSV - Arrays as JSON text
  [2] EXPLODE - Single CSV - One row per nested item  
  [3] RELATIONAL - Multiple CSVs - 3 linked files
      OUTPUT FILES: main.csv, projects.csv, tasks.csv

  Enter your choice [1/2/3]:
```

## Project Structure

```
python-json-to-csv-pipeline/
├── src/
│   ├── pipeline.py       # Main pipeline orchestrator
│   ├── extractor.py      # JSON extraction
│   ├── transformer.py    # Data cleaning & deduplication
│   ├── loader.py         # CSV writing
│   ├── analyzer.py       # JSON structure analysis
│   ├── preview.py        # Interactive preview generator
│   ├── logger_config.py  # Logging setup
│   └── modes/            # Conversion mode implementations
│       ├── flat.py       # FLAT mode converter
│       ├── explode.py    # EXPLODE mode converter
│       └── relational.py # RELATIONAL mode converter
├── data/
│   ├── input/            # Put your JSON files here
│   └── output/           # CSV files appear here
├── logs/                 # Log files
├── config/
│   └── config.yaml       # Optional settings
├── run_pipeline.py       # CLI entry point
└── requirements.txt      # Dependencies
```

## Performance

Tested with 50MB JSON file (28,212 records):

| Mode | Output Files | Total Rows | Time |
|------|-------------|------------|------|
| FLAT | 1 | 28,212 | 1.5s |
| EXPLODE | 1 | ~112,000 | 2.0s |
| RELATIONAL | 3 | 112,848 | 2.4s |

## Large File Support

For JSON files larger than 500MB, streaming is automatically used.

| File Size | Processing Method |
|-----------|------------------|
| < 500 MB | Standard (fast) |
| > 500 MB | Streaming (memory efficient) |

## Requirements

- Python 3.10+
- pandas
- polars
- pyarrow
- pyyaml
- ijson (for large files)
- tqdm (progress bars)

## Author

**nkratastr** - [GitHub](https://github.com/nkratastr)

## License

MIT License