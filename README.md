# JSON to CSV Pipeline

A simple, production-ready Python pipeline that converts **any JSON file** to CSV format automatically.

**No configuration needed** - just run it and enter your file path!

## Features

- **Works with any JSON structure** - arrays, nested objects, single records
- **Interactive mode** - just run and enter your file path
- **Large file support** - handles files over 500MB with streaming
- **Auto-detection** - automatically finds and extracts data from any JSON format
- **Error handling** - robust try-catch blocks prevent crashes
- **Logging** - all operations logged to file for debugging
- **Clean code** - modular ETL architecture (Extract, Transform, Load)

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

**Option A: Interactive Mode (Recommended)**
```bash
python run_pipeline.py
# Then enter the path to your JSON file when prompted
```

**Option B: Command Line**
```bash
python -m src.pipeline your_file.json
python -m src.pipeline data/input/sample.json -o data/output
python -m src.pipeline data/input/sample.json -f custom_name.csv
```

**Option C: Python Code**
```python
from src.pipeline import JSONToCSVPipeline

pipeline = JSONToCSVPipeline("your_file.json")
output_file = pipeline.run()
print(f"CSV created: {output_file}")
```

## Supported JSON Formats

The pipeline automatically handles all these formats:

**1. Simple Array:**
```json
[
  {"id": 1, "name": "John"},
  {"id": 2, "name": "Jane"}
]
```

**2. Nested Object:**
```json
{
  "data": [
    {"product": "Laptop", "price": 999},
    {"product": "Mouse", "price": 25}
  ]
}
```

**3. Single Record:**
```json
{"id": 1, "name": "John", "email": "john@example.com"}
```

## Project Structure

```
python-json-to-csv-pipeline/
├── src/
│   ├── pipeline.py      # Main pipeline (orchestrator)
│   ├── extractor.py     # Reads JSON files
│   ├── transformer.py   # Cleans data
│   ├── loader.py        # Writes CSV files
│   └── logger_config.py # Logging setup
├── data/
│   ├── input/           # Put your JSON files here
│   └── output/          # CSV files appear here
├── logs/                # Log files
├── config/
│   └── config.yaml      # Optional settings
├── run_pipeline.py      # Simple runner script
├── examples.py          # Usage examples
└── requirements.txt     # Dependencies
```

## Example Output

```
Pipeline Execution Summary:
---------------------------
Input Records:      5
Output Records:     5
Columns:            8
Column Names:       id, name, email, age, city, country, registration_date, is_active
Output File:        data/output/output_20260205_162530.csv
File Size:          0.45 KB
```

## Large File Support

For JSON files larger than 500MB, the pipeline automatically uses streaming (ijson library) to process the file without loading it entirely into memory.

| File Size | Processing Method |
|-----------|------------------|
| < 500 MB | Standard (fast) |
| > 500 MB | Streaming (memory efficient) |

## Logging

All operations are logged to `logs/pipeline.log`:

```
2026-02-05 16:25:30 - INFO - Pipeline execution started
2026-02-05 16:25:30 - INFO - Step 1/3: Extracting data from JSON
2026-02-05 16:25:30 - INFO - Extracted: 5 records, 8 columns
2026-02-05 16:25:30 - INFO - Step 2/3: Transforming data
2026-02-05 16:25:30 - INFO - Transformed: 5 records
2026-02-05 16:25:30 - INFO - Step 3/3: Writing to CSV
2026-02-05 16:25:30 - INFO - Pipeline completed successfully!
```

## Requirements

- Python 3.10+
- pandas
- pyyaml
- ijson (for large files)

## Author

**nkratastr** - [GitHub](https://github.com/nkratastr)

## License

MIT License