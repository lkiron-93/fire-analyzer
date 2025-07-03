"""
FIRE - Financial Institution Regulatory Extractor
A comprehensive GUI application for extracting and analyzing regulatory filings and financial documents

Features:
- Multi-company support with SEC EDGAR API integration
- Support for various filing types (10-K, 10-Q, 8-K, etc.)
- Call Report support (PDF, XBRL, SDF formats)
- Local file analysis with document type identification
- Advanced table extraction with formatting preservation
- Multiple export formats (Excel, CSV, JSON)
- Modern dark theme UI with intuitive navigation

"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import subprocess
import sys
import os
import threading
import queue
import json
from datetime import datetime
import webbrowser

# Global dictionary to store imported libraries after verification
imported_libs = {}

def check_and_import_libraries():
    """
    Check if required libraries are installed and import them dynamically
    
    This function attempts to import all required packages and keeps track
    of any missing ones. It uses dynamic imports to avoid errors if packages
    are not installed.
    
    Returns:
        list: Names of packages that failed to import
    """
    required_packages = {
        'requests': 'requests',           # HTTP library for API calls
        'pandas': 'pandas',               # Data manipulation
        'beautifulsoup4': 'bs4',          # HTML/XML parsing
        'openpyxl': 'openpyxl',          # Excel file handling
        'lxml': 'lxml',                   # XML parser
        'numpy': 'numpy',                 # Numerical operations
        'yfinance': 'yfinance',           # Yahoo Finance API (optional)
        'xlsxwriter': 'xlsxwriter',       # Enhanced Excel formatting
        'pdfplumber': 'pdfplumber'        # PDF parsing for Call Reports
    }
    
    missing_packages = []
    
    for package_name, import_name in required_packages.items():
        try:
            if import_name == 'bs4':
                # Special handling for BeautifulSoup
                imported_libs['BeautifulSoup'] = __import__('bs4', fromlist=['BeautifulSoup']).BeautifulSoup
            else:
                # Standard import
                imported_libs[import_name] = __import__(import_name)
        except ImportError:
            missing_packages.append(package_name)
    
    return missing_packages

class FIREAnalyzer:
    """
    Main application class for the FIRE (Financial Institution Regulatory Extractor) GUI
    
    This class creates and manages the entire GUI application, including:
    - Tab-based interface for different workflows
    - Library installation management
    - Filing analysis coordination
    - Export functionality
    - Thread management for non-blocking operations
    """
    
    def __init__(self, root):
        """
        Initialize the FIRE Analyzer application
        
        Args:
            root: Tkinter root window object
        """
        self.root = root
        self.root.title("🔥 FIRE - Financial Institution Regulatory Extractor")
        self.root.geometry("900x700")
        
        # Try to set application icon if available
        try:
            self.root.iconbitmap('icon.ico')
        except:
            pass  # Icon file not found, continue without it
        
        # Initialize the style system
        self.style = ttk.Style()
        self.style.theme_use('clam')  # Modern theme

        # Define color scheme for dark theme
        # These colors create a professional, modern appearance
        self.bg_color = "#1e1e1e"          # Dark background
        self.card_color = "#2d2d2d"        # Card backgrounds
        self.primary_color = "#007acc"     # Blue accent (Microsoft VSCode blue)
        self.secondary_color = "#4fc3f7"   # Light blue
        self.success_color = "#4caf50"     # Green for success states
        self.error_color = "#f44336"       # Red for errors
        self.text_color = "#ffffff"        # White text
        self.muted_color = "#b0bec5"       # Muted/secondary text

        # Apply the color scheme
        self.root.configure(bg=self.bg_color)
        self.configure_modern_styles()
        
        # Initialize queue for thread communication
        # This allows background threads to send updates to the GUI
        self.queue = queue.Queue()
        
        # Check which required libraries are missing
        self.missing_packages = check_and_import_libraries()
        
        # Build the GUI interface
        self.create_gui()
        
        # Start monitoring the queue for messages from background threads
        self.root.after(100, self.process_queue)
        
    def configure_modern_styles(self):
        """
        Configure TTK styles for a modern, professional appearance
        
        This method sets up custom styles for all TTK widgets used in the
        application, creating a cohesive dark theme design.
        """
        # Configure notebook (tab container) style
        self.style.configure('TNotebook', 
                            background=self.bg_color,
                            borderwidth=0)
        self.style.configure('TNotebook.Tab',
                            background=self.card_color,
                            foreground=self.text_color,
                            padding=[20, 10],
                            font=('Arial', 10, 'bold'))
        # Tab appearance changes on hover and selection
        self.style.map('TNotebook.Tab',
                      background=[('selected', self.primary_color),
                                ('active', self.secondary_color)])
        
        # Configure frame styles for grouping elements
        self.style.configure('TLabelFrame',
                            background=self.bg_color,
                            foreground=self.text_color,
                            borderwidth=1,
                            relief='solid')
        self.style.configure('TLabelFrame.Label',
                            background=self.bg_color,
                            foreground=self.primary_color,
                            font=('Arial', 11, 'bold'))
        
        # Configure button styles
        self.style.configure('TButton',
                            background=self.primary_color,
                            foreground='white',
                            borderwidth=0,
                            focuscolor='none',
                            font=('Arial', 10))
        # Button appearance changes on interaction
        self.style.map('TButton',
                      background=[('active', self.secondary_color),
                                ('pressed', '#005999')])
        
        # Configure entry widget styles
        self.style.configure('TEntry',
                            fieldbackground=self.card_color,
                            foreground=self.text_color,
                            borderwidth=1,
                            insertcolor=self.text_color)
        
        # Configure combobox (dropdown) styles
        self.style.configure('TCombobox',
                            fieldbackground=self.card_color,
                            foreground=self.text_color,
                            borderwidth=1)
        
    def create_gui(self):
        """
        Create the main GUI interface with three tabs
        
        The interface is organized into three main tabs:
        1. Setup & Configuration - Library installation and instructions
        2. Filing Analysis - Company search and document analysis
        3. Results & Export - View results and export options
        """
        # Create the main notebook widget for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Create frames for each tab
        self.setup_tab = ttk.Frame(self.notebook)
        self.filing_tab = ttk.Frame(self.notebook)
        self.results_tab = ttk.Frame(self.notebook)
        
        # Add tabs to notebook
        self.notebook.add(self.setup_tab, text="Setup & Configuration")
        self.notebook.add(self.filing_tab, text="Filing Analysis")
        self.notebook.add(self.results_tab, text="Results & Export")
        
        # Populate each tab with its content
        self.create_setup_tab()
        self.create_filing_tab()
        self.create_results_tab()
        
        # Disable filing and results tabs if required packages are missing
        # This prevents errors from attempting to use uninstalled libraries
        if self.missing_packages:
            self.notebook.tab(1, state='disabled')
            self.notebook.tab(2, state='disabled')
    
    def create_setup_tab(self):
        """
        Create the setup and configuration tab
        
        This tab contains:
        - Application title and branding
        - Library installation status and controls
        - User guide and instructions
        """
        # Title section with white background for contrast
        title_frame = tk.Frame(self.setup_tab, bg='white', height=80)
        title_frame.pack(fill='x', padx=20, pady=(20, 10))
        title_frame.pack_propagate(False)  # Maintain fixed height
        
        # Main application title
        title_label = tk.Label(
            title_frame,
            text="🔥 FIRE Analyzer",
            font=('Arial', 24, 'bold'),
            bg='white',
            fg=self.primary_color
        )
        title_label.pack(pady=20)
        
        # Subtitle
        subtitle_label = tk.Label(
            title_frame,
            text="Financial Institution Regulatory Extractor",
            font=('Arial', 12),
            bg='white',
            fg='gray'
        )
        subtitle_label.pack()
        
        # Library Status Frame - Shows installation status
        lib_frame = ttk.LabelFrame(self.setup_tab, text="Library Status", padding=20)
        lib_frame.pack(fill='x', padx=20, pady=10)
        
        if self.missing_packages:
            # Show missing packages and install button
            status_text = f"Missing packages: {', '.join(self.missing_packages)}"
            status_color = self.error_color
            
            # Create install button
            self.install_button = tk.Button(
                lib_frame,
                text="Install Required Libraries",
                command=self.install_packages,
                bg=self.secondary_color,
                fg='white',
                font=('Arial', 12, 'bold'),
                padx=20,
                pady=10,
                cursor='hand2'
            )
            self.install_button.pack(pady=10)
        else:
            # All packages installed
            status_text = "✓ All required libraries are installed"
            status_color = self.success_color
        
        # Status label showing current state
        self.status_label = tk.Label(
            lib_frame,
            text=status_text,
            font=('Arial', 12),
            fg=status_color
        )
        self.status_label.pack(pady=5)
        
        # Progress indicators (hidden until installation starts)
        self.progress_frame = tk.Frame(lib_frame)
        self.progress_frame.pack(fill='x', pady=10)
        
        self.progress_bar = ttk.Progressbar(
            self.progress_frame,
            mode='indeterminate',
            length=400
        )
        
        self.progress_label = tk.Label(
            self.progress_frame,
            text="",
            font=('Arial', 10)
        )
        
        # User guide section
        instructions_frame = ttk.LabelFrame(self.setup_tab, text="Getting Started", padding=20)
        instructions_frame.pack(fill='both', expand=True, padx=20, pady=10)
        
        # Comprehensive instructions for users
        instructions = """
🔥 FIRE (Financial Institution Regulatory Extractor) - User Guide

📋 QUICK START:
1. Install Libraries: Click 'Install Required Libraries' if needed
2. Go to 'Filing Analysis' tab to begin

📊 TWO ANALYSIS METHODS:

METHOD 1: Live SEC API (Recommended for SEC Filings)
- Uses your email for SEC API authentication 
- Enter company ticker (e.g., AAPL, COF, MSFT)
- Click 'Search' then 'Analyze Filing'
- Downloads latest filings automatically from SEC.gov

METHOD 2: Local File Upload (For SEC Filings & Call Reports)
- Download filing manually first
- Use 'Local File' option in Filing Analysis tab
- Specify document type (Call Report, 10-K, etc.)
- Supported formats:
  • SEC Filings: HTML (.htm), XBRL (.xbrl), XML (.xml)
  • Call Reports: PDF (.pdf), XBRL (.xbrl), SDF (.sdf, .txt)

📁 WHERE TO GET FILES:
- SEC Filings: sec.gov/edgar/search-filings
- Call Reports: cdr.ffiec.gov/public/
  • Download options: XBRL, PDF, or SDF formats
  • All three Call Report formats are supported!

✨ KEY FEATURES:
- Automatic financial table extraction
- Excel export with original formatting preserved
- Support for 10-K, 10-Q, 8-K, and Call Reports
- MDRM dictionary integration (8,863+ codes)
- Handles XBRL, PDF, and SDF Call Report formats
- Professional-grade analysis tools
- Custom company naming for exports

📊 CALL REPORT FEATURES:
- XBRL: Best for structured data extraction
- PDF: Best for preserving visual layout with schedule detection
- SDF: Best for raw data processing
- Auto-populates blank descriptions using MDRM dictionary

⚠️ NOTE: Live API requires email authentication (already configured)
"""
        
        # Create scrollable text widget for instructions
        instructions_text = tk.Text(
            instructions_frame,
            wrap='word',
            height=12,
            font=('Arial', 10),
            bg='white'
        )
        instructions_text.pack(fill='both', expand=True)
        instructions_text.insert('1.0', instructions)
        instructions_text.config(state='disabled')  # Make read-only
    
    def create_filing_tab(self):
        """
        Create the filing analysis tab
        
        This tab contains:
        - Company search functionality
        - Document source selection (API vs local file)
        - Filing type selection
        - Analysis options
        - Progress monitoring
        """
        # Company selection frame - for searching companies
        company_frame = ttk.LabelFrame(self.filing_tab, text="Company Selection", padding=15)
        company_frame.pack(fill='x', padx=20, pady=(20, 10))
        
        # Search method selection (ticker, name, or CIK)
        tk.Label(company_frame, text="Search by:", font=('Arial', 10)).grid(row=0, column=0, sticky='w', padx=5)
        
        self.search_method = tk.StringVar(value="ticker")
        
        # Radio buttons for search method
        tk.Radiobutton(
            company_frame,
            text="Ticker Symbol",
            variable=self.search_method,
            value="ticker",
            command=self.update_search_field
        ).grid(row=0, column=1, padx=5)
        
        tk.Radiobutton(
            company_frame,
            text="Company Name",
            variable=self.search_method,
            value="name",
            command=self.update_search_field
        ).grid(row=0, column=2, padx=5)
        
        tk.Radiobutton(
            company_frame,
            text="CIK Number",
            variable=self.search_method,
            value="cik",
            command=self.update_search_field
        ).grid(row=0, column=3, padx=5)
        
        # Search input field
        tk.Label(company_frame, text="Enter Value:", font=('Arial', 10)).grid(row=1, column=0, sticky='w', padx=5, pady=10)
        
        self.search_entry = ttk.Entry(company_frame, width=30, font=('Arial', 10))
        self.search_entry.grid(row=1, column=1, columnspan=2, padx=5, pady=10)
        
        # Search button
        self.search_button = ttk.Button(
            company_frame,
            text="Search",
            command=self.search_company
        )
        self.search_button.grid(row=1, column=3, padx=5, pady=10)
        
        # Company info display (shows search results)
        self.company_info_label = tk.Label(
            company_frame,
            text="",
            font=('Arial', 10),
            fg=self.primary_color
        )
        self.company_info_label.grid(row=2, column=0, columnspan=4, pady=5)
        
        # Document selection frame
        doc_frame = ttk.LabelFrame(self.filing_tab, text="Document Selection", padding=15)
        doc_frame.pack(fill='x', padx=20, pady=10)
        
        # Document source selection (SEC API or local file)
        tk.Label(doc_frame, text="Source:", font=('Arial', 10)).grid(row=0, column=0, sticky='w', padx=5)
        
        self.doc_source = tk.StringVar(value="sec")
        
        tk.Radiobutton(
            doc_frame,
            text="SEC Filings",
            variable=self.doc_source,
            value="sec",
            command=self.update_doc_types
        ).grid(row=0, column=1, padx=5)
        
        tk.Radiobutton(
            doc_frame,
            text="Local File",
            variable=self.doc_source,
            value="local",
            command=self.update_doc_types
        ).grid(row=0, column=2, padx=5)
        
        # Filing type dropdown (for SEC API)
        tk.Label(doc_frame, text="Filing Type:", font=('Arial', 10)).grid(row=1, column=0, sticky='w', padx=5, pady=10)
        
        self.filing_type = tk.StringVar()
        self.filing_dropdown = ttk.Combobox(
            doc_frame,
            textvariable=self.filing_type,
            width=28,
            state='readonly'
        )
        self.filing_dropdown.grid(row=1, column=1, columnspan=2, padx=5, pady=10)
        self.update_doc_types()  # Initialize dropdown values
        
        # Date range selection
        tk.Label(doc_frame, text="Date Range:", font=('Arial', 10)).grid(row=2, column=0, sticky='w', padx=5)
        
        self.date_range = tk.StringVar(value="latest")
        ttk.Combobox(
            doc_frame,
            textvariable=self.date_range,
            values=["Latest", "Last 2 Years", "Last 5 Years", "All Available"],
            width=28,
            state='readonly'
        ).grid(row=2, column=1, columnspan=2, padx=5)
        
        # Local file selection frame (hidden by default)
        self.local_file_frame = tk.Frame(doc_frame)
        self.local_file_frame.grid(row=3, column=0, columnspan=4, pady=10)

        # Row 1: File selection and document type on same row
        file_row_frame = tk.Frame(self.local_file_frame)
        file_row_frame.pack(fill='x')

        # File path section
        tk.Label(
            file_row_frame,
            text="File:",
            font=('Arial', 10)
        ).pack(side='left', padx=(5, 5))

        self.file_path_var = tk.StringVar()
        self.file_entry = ttk.Entry(
            file_row_frame,
            textvariable=self.file_path_var,
            width=35
        )
        self.file_entry.pack(side='left', padx=(0, 5))

        ttk.Button(
            file_row_frame,
            text="Browse",
            command=self.browse_file
        ).pack(side='left', padx=(0, 15))

        # Document type section on same row
        tk.Label(
            file_row_frame,
            text="Document Type:",
            font=('Arial', 10)
        ).pack(side='left', padx=(10, 5))

        self.local_doc_type = tk.StringVar()
        self.doc_type_entry = ttk.Entry(
            file_row_frame,
            textvariable=self.local_doc_type,
            width=20,
            font=('Arial', 10)
        )
        self.doc_type_entry.pack(side='left')

        # Row 2: Quick select buttons
        quick_select_frame = tk.Frame(self.local_file_frame)
        quick_select_frame.pack(fill='x', pady=(5, 0))

        tk.Label(
            quick_select_frame,
            text="Quick select:",
            font=('Arial', 9),
            fg='gray'
        ).pack(side='left', padx=(5, 10))

        # Create buttons for common document types
        for doc_type in ["Call Report", "10-K", "10-Q", "8-K"]:
            tk.Button(
                quick_select_frame,
                text=doc_type,
                command=lambda dt=doc_type: self.local_doc_type.set(dt),
                bg=self.secondary_color,
                fg='white',
                font=('Arial', 9),
                padx=15,
                pady=2,
                cursor='hand2',
                relief='flat'
            ).pack(side='left', padx=2)

        # Initially hide local file frame
        self.local_file_frame.grid_remove()
        
        # Analysis options frame
        options_frame = ttk.LabelFrame(self.filing_tab, text="Analysis Options", padding=15)
        options_frame.pack(fill='x', padx=20, pady=10)
        
        # Checkbox options for analysis
        self.extract_tables = tk.BooleanVar(value=True)
        self.extract_text = tk.BooleanVar(value=False)
        self.preserve_formatting = tk.BooleanVar(value=True)
        self.create_summary = tk.BooleanVar(value=True)
        
        tk.Checkbutton(
            options_frame,
            text="Extract Financial Tables",
            variable=self.extract_tables
        ).grid(row=0, column=0, sticky='w', padx=10, pady=5)
        
        tk.Checkbutton(
            options_frame,
            text="Extract Text Sections",
            variable=self.extract_text
        ).grid(row=0, column=1, sticky='w', padx=10, pady=5)
        
        tk.Checkbutton(
            options_frame,
            text="Preserve Original Formatting",
            variable=self.preserve_formatting
        ).grid(row=1, column=0, sticky='w', padx=10, pady=5)
        
        tk.Checkbutton(
            options_frame,
            text="Create Summary Report",
            variable=self.create_summary
        ).grid(row=1, column=1, sticky='w', padx=10, pady=5)
        
        # Main analyze button
        self.analyze_button = tk.Button(
            self.filing_tab,
            text="Analyze Filing",
            command=self.analyze_filing,
            bg=self.success_color,
            fg='white',
            font=('Arial', 14, 'bold'),
            padx=30,
            pady=12,
            cursor='hand2'
        )
        self.analyze_button.pack(pady=20)
        
        # Progress display area
        self.analysis_progress = scrolledtext.ScrolledText(
            self.filing_tab,
            height=8,
            wrap='word',
            font=('Courier', 9)
        )
        self.analysis_progress.pack(fill='both', expand=True, padx=20, pady=(0, 20))
    
    def create_results_tab(self):
        """
        Create the results and export tab
        
        This tab contains:
        - Analysis summary display
        - Company identifier input for file naming
        - Export format options
        - Output location selection
        - Results preview table
        """
        # Results summary frame
        summary_frame = ttk.LabelFrame(self.results_tab, text="Analysis Summary", padding=15)
        summary_frame.pack(fill='x', padx=20, pady=(20, 10))
        
        # Text widget for summary display
        self.summary_text = tk.Text(
            summary_frame,
            height=6,
            wrap='word',
            font=('Arial', 10)
        )
        self.summary_text.pack(fill='x')
        
        # Export options frame
        export_frame = ttk.LabelFrame(self.results_tab, text="Export Options", padding=15)
        export_frame.pack(fill='x', padx=20, pady=10)

        # Company name input for file naming
        company_name_frame = tk.Frame(export_frame)
        company_name_frame.grid(row=0, column=0, columnspan=4, sticky='w', pady=(0, 10))

        tk.Label(
            company_name_frame, 
            text="Company Identifier for Filename:", 
            font=('Arial', 10, 'bold')
        ).pack(side='left', padx=5)

        self.company_identifier = tk.StringVar()
        self.company_entry = ttk.Entry(
            company_name_frame,
            textvariable=self.company_identifier,
            width=30,
            font=('Arial', 10)
        )
        self.company_entry.pack(side='left', padx=5)

        tk.Label(
            company_name_frame,
            text="(e.g., COF, Capital_One, JPM)",
            font=('Arial', 9),
            fg='gray'
        ).pack(side='left', padx=5)

        # Export format selection
        tk.Label(export_frame, text="Export Format:", font=('Arial', 10)).grid(row=1, column=0, sticky='w', padx=5)
        
        self.export_format = tk.StringVar(value="excel_formatted")
        formats = [
            ("Excel with Formatting", "excel_formatted"),
            ("Excel Basic", "excel_basic"),
            ("CSV Files", "csv"),
            ("JSON", "json"),
            ("All Formats", "all")
        ]
        
        # Create radio buttons for each format
        for i, (text, value) in enumerate(formats):
            tk.Radiobutton(
                export_frame,
                text=text,
                variable=self.export_format,
                value=value
            ).grid(row=(i//3) + 1, column=(i%3)+1, sticky='w', padx=10, pady=5)
        
        # Output folder selection
        tk.Label(export_frame, text="Output Folder:", font=('Arial', 10)).grid(row=3, column=0, sticky='w', padx=5, pady=10)
        
        # Default to user's desktop
        self.output_path_var = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "Desktop", "FIRE_Analysis"))
        
        output_frame = tk.Frame(export_frame)
        output_frame.grid(row=3, column=1, columnspan=3, sticky='w', pady=10)
        
        ttk.Entry(
            output_frame,
            textvariable=self.output_path_var,
            width=50
        ).pack(side='left', padx=5)
        
        ttk.Button(
            output_frame,
            text="Browse",
            command=self.browse_output_folder
        ).pack(side='left')
        
        # Export button
        self.export_button = tk.Button(
            export_frame,
            text="Export Results",
            command=self.export_results,
            bg=self.secondary_color,
            fg='white',
            font=('Arial', 12, 'bold'),
            padx=20,
            pady=10,
            cursor='hand2',
            state='disabled'  # Disabled until analysis completes
        )
        self.export_button.grid(row=4, column=0, columnspan=4, pady=20)
        
        # Results preview frame
        preview_frame = ttk.LabelFrame(self.results_tab, text="Results Preview", padding=15)
        preview_frame.pack(fill='both', expand=True, padx=20, pady=(0, 20))
        
        # Create treeview widget for table listing
        self.results_tree = ttk.Treeview(preview_frame, height=10)
        self.results_tree.pack(fill='both', expand=True)
        
        # Add scrollbars to treeview
        v_scroll = ttk.Scrollbar(preview_frame, orient='vertical', command=self.results_tree.yview)
        v_scroll.pack(side='right', fill='y')
        self.results_tree.configure(yscrollcommand=v_scroll.set)
        
        h_scroll = ttk.Scrollbar(preview_frame, orient='horizontal', command=self.results_tree.xview)
        h_scroll.pack(side='bottom', fill='x')
        self.results_tree.configure(xscrollcommand=h_scroll.set)
    
    def install_packages(self):
        """
        Initiate package installation process
        
        This method starts the installation of missing packages in a separate
        thread to avoid freezing the GUI.
        """
        # Disable install button to prevent multiple clicks
        self.install_button.config(state='disabled')
        
        # Show progress indicators
        self.progress_bar.pack()
        self.progress_bar.start()
        self.progress_label.pack(pady=5)
        
        # Run installation in separate thread
        thread = threading.Thread(target=self._install_packages_thread)
        thread.daemon = True  # Thread will close when main program exits
        thread.start()
    
    def _install_packages_thread(self):
        """
        Thread function to install missing packages using pip
        
        This runs in the background to install each missing package
        and sends status updates to the main GUI thread via the queue.
        """
        try:
            for package in self.missing_packages:
                # Send progress update
                self.queue.put(('progress', f"Installing {package}..."))
                
                # Run pip install command
                result = subprocess.run(
                    [sys.executable, "-m", "pip", "install", package],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode != 0:
                    # Installation failed
                    self.queue.put(('error', f"Failed to install {package}: {result.stderr}"))
                    return
            
            # All packages installed successfully
            self.queue.put(('success', "All packages installed successfully!"))
            
            # Re-check imports
            global imported_libs
            missing = check_and_import_libraries()
            
            if not missing:
                # Enable other tabs if all packages now available
                self.queue.put(('enable_tabs', None))
                
        except Exception as e:
            self.queue.put(('error', f"Installation error: {str(e)}"))
    
    def process_queue(self):
        """
        Process messages from background threads
        
        This method runs periodically to check for messages from background
        threads and update the GUI accordingly. It handles various message
        types for different operations.
        """
        try:
            while True:
                # Get message from queue (non-blocking)
                msg_type, msg_data = self.queue.get_nowait()
                
                if msg_type == 'progress':
                    # Update progress label and analysis progress
                    self.progress_label.config(text=msg_data)
                    self.update_analysis_progress(msg_data)
                
                elif msg_type == 'success':
                    # Installation completed successfully
                    self.progress_bar.stop()
                    self.progress_bar.pack_forget()
                    self.progress_label.config(text=msg_data, fg=self.success_color)
                    self.status_label.config(
                        text="✓ All required libraries are installed",
                        fg=self.success_color
                    )
                    self.install_button.pack_forget()
                
                elif msg_type == 'error':
                    # Handle errors
                    self.progress_bar.stop()
                    self.progress_bar.pack_forget()
                    messagebox.showerror("Error", msg_data)
                    if hasattr(self, 'install_button'):
                        self.install_button.config(state='normal')
                
                elif msg_type == 'enable_tabs':
                    # Enable filing and results tabs
                    self.notebook.tab(1, state='normal')
                    self.notebook.tab(2, state='normal')
                    self.notebook.select(1)  # Switch to filing tab
                
                elif msg_type == 'analysis_complete':
                    # Analysis finished successfully
                    self.analyze_button.config(state='normal', text="Analyze Filing")
                    self.export_button.config(state='normal')
                    self.notebook.select(2)  # Switch to results tab
                    self.display_results(msg_data)
                
        except queue.Empty:
            pass  # No messages to process
        
        # Schedule next check
        self.root.after(100, self.process_queue)
    
    def update_search_field(self):
        """
        Update search field placeholder based on selected search method
        
        This provides user guidance on the expected input format.
        """
        method = self.search_method.get()
        placeholders = {
            'ticker': 'e.g., AAPL, MSFT, JPM',
            'name': 'e.g., Apple Inc., Microsoft',
            'cik': 'e.g., 0000320193'
        }
        # TODO: Implement actual placeholder update
        # Tkinter Entry doesn't support placeholders natively
    
    def update_doc_types(self):
        """
        Update document type options based on selected source
        
        Shows different options for SEC API vs local file upload.
        """
        source = self.doc_source.get()
        
        if source == 'sec':
            # SEC filing types available via API
            types = ['10-K', '10-Q', '8-K', 'DEF 14A', '20-F', 'All Types']
            self.filing_dropdown['values'] = types
            self.filing_type.set('10-K')
            # Hide local file selection
            if hasattr(self, 'local_file_frame'):
                self.local_file_frame.grid_remove()
    
        elif source == 'local':
            # Clear dropdown for local files
            self.filing_dropdown['values'] = []
            self.filing_type.set('')
            # Show local file selection
            if hasattr(self, 'local_file_frame'):
                self.local_file_frame.grid()
    
    def browse_file(self):
        """
        Open file dialog for selecting local filing document
        
        Supports various file types for SEC filings and Call Reports.
        """
        filename = filedialog.askopenfilename(
            title="Select SEC Filing or Call Report",
            filetypes=[
                ("SEC Filing files", "*.htm *.html"),
                ("XBRL files", "*.xbrl *.xml"),
                ("Call Report PDF", "*.pdf"),
                ("Call Report SDF", "*.sdf *.txt"),
                ("All supported files", "*.htm *.html *.xml *.xbrl *.pdf *.sdf *.txt"),
                ("All files", "*.*")
            ]
        )
        if filename:
            self.file_path_var.set(filename)
    
    def browse_output_folder(self):
        """
        Open folder dialog for selecting export destination
        """
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.output_path_var.set(folder)
    
    def search_company(self):
        """
        Search for company information using SEC EDGAR API
        
        This is a placeholder that would connect to the actual SEC API
        to search for companies by ticker, name, or CIK.
        """
        search_value = self.search_entry.get().strip()
        if not search_value:
            messagebox.showwarning("Input Required", "Please enter a search value")
            return
        
        # TODO: Implement actual SEC EDGAR API search
        # For now, show placeholder result
        self.company_info_label.config(
            text=f"Found: {search_value.upper()} - Example Corporation\nCIK: 0000000000 | Exchange: NYSE"
        )
    
    def update_analysis_progress(self, message):
        """
        Update the analysis progress display with timestamped message
        
        Args:
            message: Progress message to display
        """
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.analysis_progress.insert('end', f"{timestamp} - {message}\n")
        self.analysis_progress.see('end')  # Auto-scroll to bottom
    
    def analyze_filing(self):
        """
        Start the filing analysis process
        
        Validates inputs and starts analysis in a background thread.
        """
        # Validate inputs based on source
        if self.doc_source.get() == 'local':
            if not self.file_path_var.get():
                messagebox.showwarning("Input Required", "Please select a file")
                return
            # Check if document type is specified
            if not self.local_doc_type.get():
                messagebox.showwarning("Input Required", "Please specify the document type")
                return
        else:
            if not self.search_entry.get():
                messagebox.showwarning("Input Required", "Please search for a company first")
                return
        
        # Clear previous progress
        self.analysis_progress.delete('1.0', 'end')
        
        # Disable button and update text
        self.analyze_button.config(state='disabled', text="Analyzing...")
        
        # Run analysis in separate thread
        thread = threading.Thread(target=self._analyze_filing_thread)
        thread.daemon = True
        thread.start()
    
    def _analyze_filing_thread(self):
        """
        Thread function for filing analysis
        
        This performs the actual analysis using the enhanced_scraper module
        and sends progress updates to the GUI.
        """
        try:
            # Import the scraper module
            from enhanced_scraper import EnhancedFIREScraper    
            
            self.queue.put(('progress', 'Starting analysis...'))
            
            # Create scraper instance based on source
            if self.doc_source.get() == 'local':
                # Local file analysis
                # Get document type from user input
                doc_type = self.local_doc_type.get() or "Financial Document"
                
                # Create company info with document type
                company_info = {
                    'name': 'Local Company',
                    'ticker': 'LOCAL',
                    'cik': 'N/A',
                    'doc_type': doc_type
                }
                
                scraper = EnhancedFIREScraper(
                    company_info=company_info,
                    local_file_path=self.file_path_var.get()
                )
                
                # Store document type in metadata
                scraper.metadata['form_type'] = doc_type
            else:
                # SEC EDGAR API analysis
                self.queue.put(('progress', 'Fetching filing from SEC EDGAR...'))

                # Create scraper
                scraper = EnhancedFIREScraper()

                # Set company by ticker
                search_value = self.search_entry.get().strip()
                if not scraper.set_company(ticker=search_value):
                    self.queue.put(('error', f'Could not find company with ticker: {search_value}'))
                    return

                # Get filing URL
                filing_url = scraper.get_filing_url(self.filing_type.get())
                if not filing_url:
                    self.queue.put(('error', 'Could not find the requested filing'))
                    return

                scraper.filing_url = filing_url
            
            self.queue.put(('progress', 'Loading filing document...'))
            
            # Perform table extraction
            if scraper.scrape_all_tables():
                self.queue.put(('progress', f'Found {len(scraper.tables)} financial tables'))
                
                # Package results
                results = {
                    'tables': scraper.tables,
                    'summary': f"Extracted {len(scraper.tables)} tables",
                    'company': self.search_entry.get() or 'Local File',
                    'doc_type': scraper.metadata.get('form_type', 'Unknown')
                }
                
                # Store the scraper for export
                self.current_scraper = scraper
                
                self.queue.put(('analysis_complete', results))
            else:
                self.queue.put(('error', 'Failed to extract tables from filing'))
                
        except Exception as e:
            self.queue.put(('error', f'Analysis error: {str(e)}'))
            # Re-enable analyze button
            self.analyze_button.config(state='normal', text="Analyze Filing")
    
    def display_results(self, results):
        """
        Display analysis results in the results tab
        
        Args:
            results: Dictionary containing analysis results
        """
        # Store results for export
        self.current_results = results
        
        # Pre-populate company identifier if available
        if 'company' in results and results['company'] != 'Local File':
            self.company_identifier.set(results['company'])
        
        # Update summary text
        doc_type = results.get('doc_type', 'Document')
        summary = f"""
Analysis Complete!

Company: {results['company']}
Document Type: {doc_type}
Tables Found: {len(results['tables'])}

Summary:
{results['summary']}
"""
        self.summary_text.delete('1.0', 'end')
        self.summary_text.insert('1.0', summary)
        
        # Clear and update results tree
        self.results_tree.delete(*self.results_tree.get_children())
        
        # Configure columns
        self.results_tree['columns'] = ('Type', 'Rows', 'Columns', 'Section')
        self.results_tree.heading('#0', text='Table Name')
        self.results_tree.heading('Type', text='Type')
        self.results_tree.heading('Rows', text='Rows')
        self.results_tree.heading('Columns', text='Columns')
        self.results_tree.heading('Section', text='Section')
        
        # Add table information to tree
        for i, table in enumerate(results['tables']):
            data = table['data']['data']
            rows = len(data)
            cols = len(data[0]) if data else 0
            
            self.results_tree.insert(
                '',
                'end',
                text=table['name'],
                values=('Financial', rows, cols, table['section'])
            )
    
    def export_results(self):
        """
        Export results to selected format
        
        Shows progress window and handles the export process.
        """
        # Validate company identifier
        if not self.company_identifier.get().strip():
            messagebox.showwarning("Input Required", 
                                 "Please enter a company identifier for the filename")
            return
        
        export_format = self.export_format.get()
        output_path = self.output_path_var.get()
        
        # Create output directory
        os.makedirs(output_path, exist_ok=True)
        
        # Show export progress window
        progress_window = tk.Toplevel(self.root)
        progress_window.title("Exporting Results")
        progress_window.geometry("400x150")
        
        tk.Label(
            progress_window,
            text="Exporting results...",
            font=('Arial', 12)
        ).pack(pady=20)
        
        export_progress = ttk.Progressbar(
            progress_window,
            mode='indeterminate',
            length=300
        )
        export_progress.pack(pady=10)
        export_progress.start()
        
        # Perform export after short delay (for visual feedback)
        self.root.after(500, lambda: self._complete_export(progress_window, output_path))
    
    def _complete_export(self, progress_window, output_path):
        """
        Complete the export process
        
        Args:
            progress_window: Progress dialog to close
            output_path: Directory to save files
        """
        progress_window.destroy()

        # Perform actual export
        try:
            # Check if we have results to export
            if hasattr(self, 'current_results'):
                tables = self.current_results.get('tables', [])
                
                # Import the scraper module
                from enhanced_scraper import EnhancedFIREScraper
                
                # Use existing scraper if available, or create new one
                if hasattr(self, 'current_scraper'):
                    scraper = self.current_scraper
                else:
                    # Create a new scraper instance with the tables
                    scraper = EnhancedFIREScraper()
                    scraper.tables = tables
                
                # Update company info with user-specified identifier
                company_id = self.company_identifier.get().strip()
                company_id = company_id.replace(' ', '_').replace('/', '_').replace('\\', '_')
                
                scraper.company_info = {
                    'name': company_id,
                    'ticker': company_id,
                    'cik': 'N/A'
                }
                
                # Update metadata
                scraper.metadata.update({
                    'company': company_id,
                    'ticker': company_id,
                    'form_type': self.current_results.get('doc_type', '10-K')
                })
                
                # Generate filenames with company identifier
                date_str = datetime.now().strftime('%Y%m%d')
                base_filename = f"{company_id}_financial_tables_{date_str}"
                
                # Export based on selected format
                export_format = self.export_format.get()
                
                if export_format == 'excel_formatted':
                    output_file = os.path.join(output_path, f'{base_filename}_formatted.xlsx')
                    scraper.save_to_excel_formatted(output_file)
                elif export_format == 'excel_basic':
                    output_file = os.path.join(output_path, f'{base_filename}_basic.xlsx')
                    scraper.save_to_excel_basic(output_file)
                elif export_format == 'csv':
                    csv_dir = os.path.join(output_path, f'{company_id}_csv_tables_{date_str}')
                    scraper.save_to_csv(csv_dir)
                elif export_format == 'json':
                    output_file = os.path.join(output_path, f'{company_id}_financial_data_{date_str}.json')
                    scraper.save_to_json(output_file)
                elif export_format == 'all':
                    # Export all formats
                    scraper.save_to_excel_formatted(os.path.join(output_path, f'{base_filename}_formatted.xlsx'))
                    scraper.save_to_excel_basic(os.path.join(output_path, f'{base_filename}_basic.xlsx'))
                    scraper.save_to_csv(os.path.join(output_path, f'{company_id}_csv_tables_{date_str}'))
                    scraper.save_to_json(os.path.join(output_path, f'{company_id}_financial_data_{date_str}.json'))
                    
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {str(e)}")
            return
        
        # Show success message
        message = f"Results exported successfully!\n\nFiles saved to:\n{output_path}"
        result = messagebox.showinfo("Export Complete", message, type='okcancel')
        
        if result == 'ok':
            # Open output folder in file explorer
            if sys.platform == 'win32':
                os.startfile(output_path)
            elif sys.platform == 'darwin':
                subprocess.run(['open', output_path])
            else:
                subprocess.run(['xdg-open', output_path])


def main():
    """
    Main application entry point
    
    Creates the Tkinter root window and starts the application.
    """
    # Create root window
    root = tk.Tk()
    
    # Create application instance
    app = FIREAnalyzer(root)
    
    # Start the GUI event loop
    root.mainloop()


if __name__ == "__main__":
    main()