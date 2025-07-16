"""
Test configuration for FIRE Bulk Data Processing
Update these paths to match your FFIEC data location
"""

import os

# Default to sample_data directory in project
DEFAULT_DATA_DIR = os.path.join(os.path.dirname(__file__), "sample_data")

# Update this path to your FFIEC bulk data directory
# Users should modify this to point to their data location
FFIEC_DATA_DIR = os.environ.get('FFIEC_DATA_DIR', DEFAULT_DATA_DIR)

# Alternative common locations (the test will check these if the above doesn't exist)
ALTERNATIVE_PATHS = [
    # User's common download locations
    os.path.join(os.path.expanduser("~"), "Desktop", "FFIEC_Data"),
    os.path.join(os.path.expanduser("~"), "Downloads", "FFIEC_Data"),
    os.path.join(os.path.expanduser("~"), "Documents", "FFIEC_Data"),
    
    # Project sample data
    os.path.join(os.path.dirname(__file__), "sample_data"),
    
    # Common data drive locations
    "D:\\FFIEC_Data",
    "E:\\FFIEC_Data",
    
    # Look for any folder containing "FFIEC" and "CDR"
    os.path.join(os.path.expanduser("~"), "Desktop", "*FFIEC*CDR*"),
    os.path.join(os.path.expanduser("~"), "Downloads", "*FFIEC*CDR*"),
]

# Test institution RSSD IDs (public information)
TEST_RSSD_IDS = {
    "Capital One": "112837",
    "JPMorgan Chase": "852218",
    "Bank of America": "480228",
    "Wells Fargo": "451965",
    "Citibank": "476810"
}

# Dictionary path (relative to project)
DICTIONARY_PATH = os.path.join(os.path.dirname(__file__), "dictionaries", "call_report_mdrm_dictionary.json")

# Output directory for test results (relative to project)
TEST_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "test_output")

# Create output directory if it doesn't exist
os.makedirs(TEST_OUTPUT_DIR, exist_ok=True)

# Performance test settings
PERFORMANCE_SETTINGS = {
    "small_test": {
        "description": "Test with 5 files",
        "file_limit": 5,
        "use_parallel": True,
        "num_workers": 2
    },
    "medium_test": {
        "description": "Test with 20 files", 
        "file_limit": 20,
        "use_parallel": True,
        "num_workers": 4
    },
    "full_test": {
        "description": "Test with all 47 files (full quarter)",
        "file_limit": None,
        "use_parallel": True,
        "num_workers": None  # Use all available cores
    },
    "sequential_test": {
        "description": "Test sequential processing (no parallel)",
        "file_limit": 10,
        "use_parallel": False,
        "num_workers": 1
    }
}

# Test parameters
TEST_QUARTERS = ["2024-Q4", "2025-Q1"]
DEFAULT_QUARTER = "2025-Q1"

# File size thresholds (in MB)
FILE_SIZE_THRESHOLDS = {
    "small": 5,      # Files < 5MB
    "medium": 50,    # Files 5-50MB  
    "large": 100,    # Files 50-100MB
    "xlarge": 500    # Files > 100MB
}