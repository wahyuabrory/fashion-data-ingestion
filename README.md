# Fashion Data Ingestion Pipeline

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Code Coverage](https://img.shields.io/badge/coverage-95%25-brightgreen.svg)](./test_coverage_summary.md)

An ETL (Extract, Transform, Load) pipeline that scrapes fashion product data from a website, performs data cleaning and transformation, and loads the data to multiple destinations including CSV files, PostgreSQL, and Google Sheets.

![Pipeline Architecture](image/Screenshot%202025-05-09_8197.png)

## Features

- **Extract**: Scrapes product data from [fashion-studio.dicoding.dev](https://fashion-studio.dicoding.dev/) with:
  - Robust pagination handling (up to 50 pages)
  - Automatic retry logic for failed requests
  - HTML parsing with BeautifulSoup
  - Timestamp tracking for extraction time

- **Transform**: Cleans and processes the raw data with:
  - Price conversion from USD to Rupiah (rate: 16,000)
  - Rating standardization to float format
  - Color quantity extraction
  - Size and gender label cleaning
  - Data validation and incomplete record filtering

- **Load**: Outputs data to multiple destinations:
  - CSV files with consistent column formatting
  - PostgreSQL with automatic table creation
  - Google Sheets with sharing capabilities

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Testing](#testing)
- [Results](#results)
- [Contributing](#contributing)
- [License](#license)

## Requirements

- Python 3.8 or higher
- PostgreSQL 12+
- Google account with Google Sheets API enabled

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/fashion-data-ingestion.git
cd fashion-data-ingestion
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

3. Set up your PostgreSQL database (either locally or using Docker):
```bash
# Using Docker
docker run --name postgres-container -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=fashion_db -p 5432:5432 -d postgres:14
```

4. Set up your Google Sheets API credentials:
   - Create a project in [Google Cloud Console](https://console.cloud.google.com/)
   - Enable the Google Sheets and Google Drive APIs
   - Create a service account and download the credentials as `google-sheets-api.json`
   - Place the credentials file in the project root directory

## Usage

### Running the Complete Pipeline

```bash
python main.py
```

### Running with Options

```bash
# Save to CSV only (skip PostgreSQL and Google Sheets)
python main.py --csv-only

# Specify a custom CSV output path
python main.py --output-path=fashion_data.csv

# Use a custom PostgreSQL configuration file
python main.py --pg-config=my_pg_config.json

# Use a custom Google Sheets credentials file
python main.py --gs-creds=my_credentials.json
```

## Configuration

### PostgreSQL Configuration

Create a `pg_config.json` file with the following structure:

```json
{
  "host": "localhost",
  "database": "fashion_db",
  "user": "postgres",
  "password": "postgres",
  "port": "5432",
  "table_name": "fashion_data"
}
```

### Google Sheets API

Place your Google API service account credentials in a file named `google-sheets-api.json` in the project root.

## Project Structure

```
fashion-data-ingestion/
├── main.py                   # Main script to run the pipeline
├── utils/
│   ├── extract.py            # Module for data extraction from website
│   ├── transform.py          # Module for data cleaning and transformation
│   └── load.py               # Module for loading data to destinations
├── tests/                    # Test suite for ensuring code quality
├── google-sheets-api.json    # Google API credentials 
├── pg_config.json            # PostgreSQL configuration
└── products.csv              # Output CSV file
```

## Testing

The project includes a comprehensive test suite with 95% code coverage.

### Running Tests

```bash
# Run all tests
python -m pytest tests

# Run tests for a specific module
python -m pytest tests/test_extract.py
python -m pytest tests/test_transform.py
python -m pytest tests/test_load.py

# Run tests with verbose output
python -m pytest tests -v
```

### Test Coverage

```bash
# Generate coverage report
coverage run -m pytest tests
coverage report

# Generate detailed coverage report with missing lines
coverage report -m

# Generate HTML coverage report
coverage html
```

For more details, see the [Test Coverage Summary](./test_coverage_summary.md).

## Results

After running the pipeline, data will be available in the following locations:

- **CSV File**: `products.csv` in the project root
- **PostgreSQL**: Database `fashion_db`, table `fashion_data`
- **Google Sheets**: URL will be displayed in the console output after running the pipeline

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

Project developed as part of a data engineering demonstration.