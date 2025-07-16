# 🔥 FIRE - Financial Institution Regulatory Extractor

A powerful desktop application for extracting and analyzing financial data from FFIEC Call Report bulk data downloads, with advanced single and multi-institution analysis capabilities.

## 🚀 Project Status

**Current Version: 2.0** - Full-Featured Analysis Suite with Complete Single & Multi-Institution Support

### ✅ Completed Features (All Tested & Working):
1. **Excel Hyperlinks** - Schedule navigation with back links
2. **Excel Formatting** - Alternating rows, auto-fit, filters, frozen panes
3. **Name Search/Filter** - Flexible RSSD/Name input with 4,520+ institution mappings
4. **Performance Optimization** - Progress tracking and parallel processing
5. **Error Recovery** - Graceful handling of partial data and missing files
6. **Multi-Institution Comparison** - Side-by-side analysis of up to 4 institutions
7. **Executive Dashboard** - Visual charts, key ratios, and asset quality indicators
8. **Treasury Risk & ALM Metrics** - Interest rate risk, liquidity analysis, and balance sheet composition
9. **Single Institution Enhancements** - Executive Dashboard and Key Metrics for individual bank analysis

## 📋 Overview

FIRE (Financial Institution Regulatory Extractor) processes FFIEC Call Report bulk data downloads and creates sophisticated Excel reports with advanced financial analysis capabilities for both individual institutions and peer comparisons.

### Core Features:
- **FFIEC Bulk Data Processing**: Process quarterly Call Report data (47 text files per quarter)
- **Single Institution Analysis**: Deep-dive analysis with Executive Dashboard and Key Metrics
- **Multi-Institution Comparison**: Analyze up to 4 institutions side-by-side
- **Executive Dashboard**: Automated charts and key metrics visualization
- **ALM Analytics**: Treasury risk metrics including interest rate sensitivity and liquidity indicators
- **Smart Institution Lookup**: Search by RSSD ID or institution name (4,520+ institutions)
- **Professional Excel Output**: Formatted reports with hyperlinks, filters, and frozen panes
- **Resume Capability**: Continue interrupted processing and retry failed files

## 🛠️ Installation

### Prerequisites
- Python 3.7 or higher (3.9+ recommended)
- Windows, macOS, or Linux

### Quick Install
```bash
# Clone the repository
git clone https://github.com/yourusername/fire-analyzer.git
cd fire-analyzer

# Install dependencies
pip install -r requirements.txt

# Run the application
python launcher.py

Windows Users
Simply double-click run.bat to launch the application.
💡 Quick Start
Single Institution Analysis

Launch FIRE: Run launcher.py or run.bat
Select Bulk Data Processing Tab
Choose Processing Mode: Single Institution (default)
Enter Institution:

Type RSSD ID (e.g., "37") or
Type institution name (e.g., "JPMorgan" or "Bank of America")
Select from dropdown suggestions


Select Data Source: Choose directory with Call Report files
Process: Click "Process Bulk Data"
Result: Excel report with:

Formatted schedules
Executive Dashboard with visual analytics
Key Metrics sheet with financial ratios and ALM metrics
Report Info with navigation links



Multi-Institution Comparison

Select Multi-Institution Mode: Check "Multi-Institution Comparison"
Select Primary Institution: Search and select your main institution
Add 1-3 Peer Institutions: Use the search feature to add peers
Process: Creates comprehensive comparison report with:

Summary sheet with all institutions
Side-by-side schedule comparisons
Executive Dashboard with peer analysis charts
Key Metrics with comparative ratios
ALM metrics and treasury risk indicators



📊 Output Features
Excel Report Structure
Both Modes Include:

Report Info: Processing metadata, quick links to enhanced sheets
Executive Dashboard:

Total assets comparison chart
Key ratios table (ROA, ROE, NIM, Efficiency)
Interest rate risk summary
Liquidity indicators
Asset quality indicators
Top 5 balance sheet items


Key Metrics:

Profitability metrics
Efficiency metrics
Capital adequacy
Interest rate risk analysis
Liquidity metrics
Balance sheet composition
Asset quality details
Peer statistics (multi-mode) or benchmarks (single-mode)



Additional Multi-Institution Features:

Summary Sheet: Institution list with hyperlinks
Side-by-Side Comparisons: Each schedule showing all institutions
Peer Analysis: Statistical comparison across institutions

Key Metrics Analyzed

Profitability: ROA, ROE, NIM, Operating Efficiency
Asset Quality: NPL Ratio, NCO Ratio, ALL/Total Loans
Capital: Tier 1 Ratio, Leverage Ratio, Risk-Based Capital
Liquidity: Liquid Assets Ratio, Wholesale Funding, Deposit Stability
Interest Rate Risk: IR Sensitivity, Asset/Liability Yields, Gap Ratio
ALM Metrics: Earning Asset Yield, Cost of Funds, Interest Rate Spread

🔧 Technical Details
Performance Features

Parallel Processing: Multi-core utilization for faster processing
Memory Optimization: Automatic optimization for large datasets
Smart File Detection: Automatic schedule identification from filenames
Progress Tracking: Real-time updates during processing
Error Recovery: Continue processing even with missing data
Resume Support: Pick up where you left off if interrupted

Data Dictionary

8,863+ MDRM Codes: Comprehensive coverage of Call Report line items
4,520+ Institutions: Pre-loaded RSSD to name mappings
Smart Matching: Fuzzy search for institution names
Automatic Descriptions: MDRM code descriptions populated automatically

Processing Capabilities

File Size Handling: Optimized for files from 1MB to 500MB+
Chunked Processing: Large files processed in manageable chunks
Validation: Automatic data quality checks and warnings
Flexible Input: Process single files or entire quarters

🗂️ Project Structure
fire-analyzer/
├── fire_analyzer_gui.py        # Main GUI application
├── bulk_data_processor.py      # Processing engine with ExcelEnhancementProcessor
├── bulk_file_manager.py        # File organization and error recovery
├── dictionaries/               
│   ├── institution_lookup.json # 4,520+ RSSD mappings
│   └── call_report_mdrm_dictionary.json
├── logs/                       # Processing logs
├── docs/                       # Documentation
│   └── bulk_data_download_guide.md
├── test_config.py              # Test configuration
├── test_bulk_processing.py     # Test suite
├── requirements.txt            # Python dependencies
├── launcher.py                 # Application launcher
└── run.bat                     # Windows launcher


🐛 Known Limitations

Maximum 4 institutions for multi-institution comparison
Large institutions (>$1T assets) may require extended processing time
Some MDRM codes may not have descriptions (uses fallback descriptions)