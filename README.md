# 🔥 FIRE - Financial Institution Regulatory Extractor

A professional-grade Python application for extracting and analyzing financial data from regulatory filings including SEC documents, FFIEC Call Reports, and more.

## Overview

FIRE is an advanced Python application that automates the extraction, analysis, and formatting of financial data from regulatory filings. Built for financial institutions, analysts, and compliance professionals, FIRE transforms complex regulatory documents into actionable, structured data.

##  Why Use FIRE?

Born from the need to streamline financial research workflows, FIRE was created to solve a common problem: manually downloading and processing regulatory reports is time-consuming and error-prone. Whether you're analyzing a single company or conducting broader market research, FIRE transforms manual work into minutes of automated processing.

Built by a researcher, for researchers, FIRE delivers:

- ⚡ **Speed**: Extract data from complex reports in seconds, not hours
- 🔄 **Consistency**: Standardized output format across all document types
- 🎯 **Accuracy**: Automated extraction with MDRM dictionary validation
- 💼 **Professional Output**: Excel files that preserve regulatory formatting
- 🚀 **Efficiency**: Focus on analysis, not data extraction


### 🏦 Multi-Source Regulatory Support

-  **SEC EDGAR Integration**: Direct API access to SEC filings with automatic company lookup
- **FFIEC Call Reports**: Complete support for all schedules with MDRM code resolution
- **Multiple Format Handling**: Seamlessly process HTML, XBRL, PDF, and SDF formats
- **Smart PDF Processing**: Advanced detection and splitting of collapsed multi-code cells


### 📊 Intelligent Data Extraction

- **Smart Table Detection**: AI-powered identification of financial tables
- **Schedule Recognition**: Automatic Call Report schedule identification (RC, RI series)
- **MDRM Dictionary**: 8,863+ code mappings for instant line item descriptions
- **Structure Preservation**: Maintains original document hierarchy and formatting
- **Collapsed Cell Detection**: Automatically splits multi-code PDF cells into individual rows

### 💎 Professional Output

- **Excel Excellence**: Multi-sheet workbooks with preserved formatting
- **4-Column RC Format**: Special handling for RC Balance Sheet (Line Item | Description | MDRM Code | Amount)
- **Visual Hierarchy**: Automatic indentation, totals detection, and section headers
- **Flexible Formats**: Export to Excel (formatted/basic), CSV, or JSON
- **Audit Trail**: Complete metadata tracking and extraction logging

## 🚀 Quick Start

### System Requirements

- Python 3.7 or higher
- 4GB RAM minimum (8GB recommended)
- Windows 10/11, macOS 10.14+, or Linux

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/fire-analyzer.git
   cd fire-analyzer
Install dependencies
bashpip install -r requirements.txt

Launch FIRE
bashpython launcher.py
Or simply double-click run.bat on Windows.

📖 Usage Guide
Method 1: Live SEC Filing Analysis

Launch FIRE and navigate to the "Filing Analysis" tab
Search for a company by ticker, name, or CIK
Select filing type (10-K, 10-Q, 8-K, etc.)
Click "Analyze Filing" to process

Method 2: Local File Processing

Select "Local File" option
Browse to your document (PDF, XBRL, HTML, or SDF)
Specify document type (Call Report, 10-K, etc.)
Click "Analyze Filing" to extract data

Export Options

Formatted Excel: Professional workbooks with styling and structure
Basic Excel: Raw data in simple spreadsheet format
CSV: Individual files for each table
JSON: Structured data for programmatic access

🛠️ Technical Architecture
FIRE/
├── Core Modules
│   ├── enhanced_scraper.py     # Extraction engine with MDRM integration
│   ├── fire_analyzer_gui.py    # Modern GUI interface
│   └── launcher.py             # Application entry point
│
├── Data Resources
│   └── dictionaries/
│       ├── call_report_codes.json  # MDRM dictionary (8,863 codes)
│       └── MDRM_Parser/
│           └── MDRM_Parser.py      # Federal Reserve MDRM parser
│
├── Logs/                       # Extraction logs with detailed debugging
│
└── Configuration
    ├── requirements.txt        # Python dependencies
    └── run.bat                # Windows quick launcher
Key Technologies

Frontend: Tkinter with custom dark theme
Data Processing: Pandas, NumPy for efficient computation
Document Parsing: BeautifulSoup4, pdfplumber, lxml
Excel Generation: OpenPyXL with advanced formatting
API Integration: SEC EDGAR API, FFIEC data services
Logging: Comprehensive debug logging with extraction statistics

📊 Supported Documents
SEC Filings
Filing TypeDescriptionSupport Level10-KAnnual reports✅ Full10-QQuarterly reports✅ Full8-KCurrent reports✅ FullDEF 14AProxy statements✅ Full20-FForeign private issuer annual reports✅ Full
FFIEC Call Reports
ScheduleDescriptionFeaturesRC SeriesBalance Sheet schedulesAuto-detection, MDRM lookup, collapsed cell splittingRI SeriesIncome Statement schedulesFull formatting preservationAll PartsIncluding I, II sub-schedulesComplete hierarchy support
🔧 Advanced Features
Enhanced RC Balance Sheet Processing

Collapsed Cell Detection: Automatically identifies cells containing multiple MDRM codes
Smart Splitting: Converts single rows with multiple codes into individual line items
4-Column Format: Special layout for RC schedules (Line Item | Description | Code | Amount)
Debug Logging: Detailed extraction logs showing collapsed cell detection and processing

MDRM Dictionary Integration

Automatic code resolution for blank descriptions
Real-time validation during extraction
8,863+ regulatory codes mapped
Fallback handling for unmapped codes

Intelligent Formatting

Automatic indentation detection (4 levels)
Total/subtotal row identification
Section header recognition
Number formatting (thousands separator, no decimals)
Negative amount handling (parentheses to minus sign conversion)

📈 Recent Updates (v1.1.0)
New Features

✅ Collapsed Cell Detection: Automatically splits multi-code PDF cells
✅ Enhanced Debug Logging: Comprehensive extraction statistics
✅ 4-Column RC Format: Special handling for RC Balance Sheet
✅ Improved MDRM Integration: Better fallback for missing descriptions

Bug Fixes

Fixed PDF table extraction for complex Call Report layouts
Improved schedule detection regex patterns
Enhanced negative number handling

🐛 Debugging & Troubleshooting
Extraction Logs
FIRE generates detailed logs for each extraction:
logs/COMPANY_fire_extraction_YYYYMMDD_HHMMSS.log
The logs include:

Schedule detection details
Collapsed cell identification
MDRM code lookups
Row-by-row processing information
Extraction summary statistics

Common Issues

Missing MDRM descriptions

Ensure call_report_codes.json exists in the dictionaries folder
Check log for "MDRM dictionary loaded" confirmation


RC Balance Sheet formatting issues

Enable debug logging to see collapsed cell detection
Check log for "COLLAPSED CELL FOUND" messages
Verify 4-column output format in Excel


PDF extraction errors

Ensure pdfplumber is installed: pip install pdfplumber
Some scanned PDFs may not be supported (text extraction required)


### Key Updates Made:

1. **Added Collapsed Cell Detection** as a major feature
2. **Highlighted 4-Column RC Format** for Balance Sheets
3. **Added Debugging Section** with log file information
4. **Updated Version** to v1.1.0 with recent improvements
5. **Enhanced Technical Details** about the extraction process
