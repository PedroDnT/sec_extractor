# SEC Quarterly Financial Data Extractor

Automated tool to download and extract quarterly financial data from SEC 10-Q and 10-K filings.

## Features

- Downloads SEC filings automatically by ticker symbol
- Extracts Income Statement, Balance Sheet, and Cash Flow Statement
- Outputs data to Excel with quarterly columns
- Includes fiscal period dates in column headers
- Supports custom year ranges

## Setup

```bash
# Create virtual environment
uv venv .venv

# Activate environment
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```

## Usage

### Command Line
```bash
python sec_quarterly_extractor.py TICKER START_YEAR END_YEAR

# Examples
python sec_quarterly_extractor.py AAPL 2022 2025
python sec_quarterly_extractor.py MSFT 2020 2025
python sec_quarterly_extractor.py WMT 2023 2023
```

### Python Module
```python
from sec_quarterly_extractor import extract_quarterly_financials

# Extract data
output_file = extract_quarterly_financials('AAPL', 2022, 2025)
```

## Output

Generates Excel file with:
- **Income Statement** sheet
- **Balance Sheet** sheet  
- **Cash Flow Statement** sheet
- **Summary** sheet

Column format: `1Q22 (2022-04-30)` - Quarter and fiscal period end date

## Dependencies

- pandas
- requests
- beautifulsoup4
- lxml
- openpyxl
