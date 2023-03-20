# Data Engineer - Transaction Pipeline

This project processes a dataset of transactions using Apache Beam in Python. The purpose of the analysis is to filter transactions with an amount greater than 20 and a year of 2010 or later, group them by date, and calculate the total transaction amount for each date.

## Requirements

- Python 3.6+
- Apache Beam

## Installation

1. Clone the repository:
```git clone https://github.com/HenryProj/ApacheBeam.git```

2. Change to the project directory:
```cd ApacheBeam```

3. (Optional) Create a virtual environment to isolate the project dependencies:
```python -m venv venv```
```source venv/bin/activate``` P.s. On Windows, use venv\Scripts\activate

4. Install the required packages:
```pip install -r requirements.txt```


## Running the Analysis

To run the analysis using the provided sample data from Google Cloud Storage, execute the following command:
```python transaction_pipeline.py```


By default, the output will be saved in the `output/results.jsonl.gz` file.

You can provide custom input and output files using the `--input` and `--output` command-line options:
```python transaction_analysis.py --input path/to/input.csv --output path/to/output.jsonl.gz```


## Running the Tests
To run the unit tests for the `TransactionsTransform` composite transform, execute the following command:
```python transaction_pipeline_test.py```


## Author
Henry Lam


