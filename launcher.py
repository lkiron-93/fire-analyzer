#!/usr/bin/env python3
"""
FIRE - Financial Institution Regulatory Extractor - Launcher Script
Simple launcher to start the application
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 7):
        print("❌ Error: Python 3.7 or higher is required")
        print(f"   Current version: {version.major}.{version.minor}.{version.micro}")
        input("\nPress Enter to exit...")
        return False
    return True

def check_and_create_files():
    """Ensure all required files exist"""
    files_to_create = {
        'fire_analyzer_gui.py': 'Main GUI application file',  # Renamed
        'enhanced_scraper.py': 'Enhanced scraper module',
        'requirements.txt': 'Package requirements file'
    }
    
    # Create requirements.txt content
    requirements_content = """# FIRE - Financial Institution Regulatory Extractor Requirements
requests>=2.31.0
pandas>=2.0.0
beautifulsoup4>=4.12.0
openpyxl>=3.1.0
lxml>=4.9.0
numpy>=1.24.0
yfinance>=0.2.28
xlsxwriter>=3.1.0
pdfplumber>=0.9.0
tabula-py>=2.8.0
"""
    
    # Check for missing files
    missing_files = []
    for filename, description in files_to_create.items():
        if not os.path.exists(filename):
            missing_files.append(f"  - {filename}: {description}")
    
    if missing_files:
        print("❌ Missing required files:")
        for file in missing_files:
            print(file)
        print("\n📝 Please ensure all files are in the same directory as this launcher.")
        
        # Create requirements.txt if missing
        if 'requirements.txt' in [f.split(':')[0].strip() for f in missing_files]:
            with open('requirements.txt', 'w') as f:
                f.write(requirements_content)
            print("✅ Created requirements.txt")
        
        return False
    
    return True

def install_requirements():
    """Install required packages from requirements.txt"""
    print("📦 Checking and installing required packages...")
    
    try:
        # Use pip to install requirements
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "--quiet"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ All required packages are installed")
            return True
        else:
            print("⚠️  Some packages may not have installed correctly")
            print("   You can install them manually using:")
            print("   pip install -r requirements.txt")
            return True  # Continue anyway
            
    except Exception as e:
        print(f"⚠️  Could not automatically install packages: {e}")
        print("   Please install manually using: pip install -r requirements.txt")
        return True  # Continue anyway

def launch_application():
    """Launch the main GUI application"""
    print("\n🔥 Launching FIRE Analyzer...")
    
    try:
        # Run the GUI application
        subprocess.run([sys.executable, "fire_analyzer_gui.py"])  # Updated filename
        
    except KeyboardInterrupt:
        print("\n\n👋 Application closed by user")
    except Exception as e:
        print(f"\n❌ Error launching application: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure all files are in the same directory")
        print("2. Check that Python is properly installed")
        print("3. Try running: python fire_analyzer_gui.py")  # Updated
        input("\nPress Enter to exit...")

def main():
    """Main launcher function"""
    print("=" * 60)
    print("🔥 FIRE - Financial Institution Regulatory Extractor")
    print("=" * 60)
    
    # Check Python version
    if not check_python_version():
        return
    
    print(f"✅ Python {sys.version.split()[0]} detected")
    
    # Check for required files
    if not check_and_create_files():
        input("\nPress Enter to exit...")
        return
    
    # Install requirements
    install_requirements()
    
    # Launch application
    launch_application()

if __name__ == "__main__":
    # Change to script directory
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # Run main
    main()