# Real-Time Market Data Excel Updater

This module provides real-time market data updates to an Excel spreadsheet using a thread-safe implementation. It maintains a single Excel instance and updates the data continuously without opening and closing Excel repeatedly.

## Features

- Real-time market data updates in Excel
- Thread-safe implementation using a dedicated Excel worker thread
- Color-coded display of positive/negative changes
- Automatic column formatting and cell styling
- Graceful cleanup and resource management

## Requirements

- Windows OS (required for COM interface)
- Python 3.7+
- Excel installed on the system
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository:
```bash
git clone https://github.com/AshutoshBuilds/Market-with-Excel.git
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Run the monitor script:
```bash
run_monitor.bat
```

The script will:
- Create a new Excel workbook
- Set up the required formatting
- Start updating market data in real-time
- Display the following indices:
  - NIFTY 50
  - NIFTY BANK
  - NIFTY FIN SERVICE
  - NIFTY MID SELECT
  - SENSEX

## File Structure

```
excel/
├── README.md
├── requirements.txt
├── run_monitor.bat
├── run_excel_monitor.py
└── updater.py
```

## Implementation Details

- Uses COM interface through `pywin32` for Excel communication
- Implements a dedicated worker thread for Excel operations
- Uses a queue system for thread-safe data updates
- Handles proper cleanup of Excel resources
- Includes automatic restart capability through the batch script

## Error Handling

The module includes comprehensive error handling:
- COM interface initialization errors
- Excel connection issues
- Data update errors
- Proper resource cleanup

## Notes

- The Excel file will remain open while the script is running
- Updates are rate-limited to prevent excessive Excel operations
- The batch script automatically restarts the monitor if it crashes
- All Excel operations are performed in a single dedicated thread to prevent COM threading issues

## Author

Ashutosh Shukla (ashutoshshukla734.as@gmail.com) 