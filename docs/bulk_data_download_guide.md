# FFIEC Bulk Data Download Guide

## Quick Start

This guide walks you through downloading FFIEC Call Report bulk data for use with FIRE Analyzer.

## Step 1: Access the FFIEC Bulk Data Website

1. Open your web browser
2. Navigate to: https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx

## Step 2: Select Data Type

For Call Report analysis, select:
- **Option 1: Call Reports -- Single Period** (Currently supported by FIRE)

![Data Type Selection](docs/images/ffiec_data_type.png)

*Note: The other 5 options will be supported in future versions*

## Step 3: Choose Reporting Period

1. In the **"Reporting Period End Date"** dropdown, select your desired quarter
   - Example: `03/31/2025` for Q1 2025
   - Most recent data is typically 45-60 days after quarter end

## Step 4: Select File Format

**Important**: Choose **"Tab Delimited"** from the Available File Formats dropdown

## Step 5: Download the Data

1. Click the **"Download"** button in the "Bulk Data Download" section (top left)
2. A ZIP file will download (approximately 35-40 MB)
   - Filename example: `FFIEC CDR Call Bulk All Schedules 03312025.zip`

## Step 6: Extract the Files

1. Locate the downloaded ZIP file (usually in your Downloads folder)
2. Extract/unzip the contents to a folder you can easily find
   
   **Recommended locations:**
   - `C:\FFIEC_Data\[Quarter]` (Windows)
   - `~/Documents/FFIEC_Data/[Quarter]` (Mac/Linux)
   - Your Desktop (for easy access)

## Step 7: Verify the Download

Your extracted folder should contain approximately 47-48 files:
- 47 schedule files (RC, RI, etc.) in .txt format
- 1 Readme.txt file

Example files:
```
FFIEC CDR Call Schedule RC 03312025.txt
FFIEC CDR Call Schedule RI 03312025.txt
FFIEC CDR Call Schedule RCN 03312025.txt
... (44 more schedule files)
Readme.txt
```

## Using with FIRE Analyzer

1. Open FIRE Analyzer
2. Go to the **"Bulk Data Processing"** tab
3. Select **"Directory (Multiple Files)"** mode
4. Click **"Browse"** and navigate to your extracted folder
5. Configure your options:
   - Filter by RSSD ID (optional)
   - Select institution name
6. Click **"Process Bulk Data"**

## Tips

- **Storage**: Each quarter's data is ~35MB compressed, ~50MB extracted
- **Organization**: Create a folder structure like:
  ```
  FFIEC_Data/
  ├── 2025_Q1/
  ├── 2025_Q2/
  └── 2025_Q3/
  ```
- **Updates**: New data is typically available 45-60 days after quarter end
- **Multiple Quarters**: Download each quarter separately for trend analysis

## Troubleshooting

**"No files found" error:**
- Ensure you extracted the ZIP file (don't browse inside the ZIP)
- Check that you selected the folder containing the .txt files

**"Invalid file format" error:**
- Verify you downloaded "Tab Delimited" format, not XML or other formats
- Check that files have .txt extension

**Missing schedules:**
- Some institutions may not file all schedules
- Check the Readme.txt for data collection date

## Need Help?

If you encounter issues:
1. Check the FFIEC website for data availability
2. Ensure you have the latest version of FIRE Analyzer
3. Review the log files in the `logs/` folder

---
*Last Updated: July 2025*