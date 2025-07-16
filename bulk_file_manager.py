"""
FIRE Bulk Data File Management Module
Handles automated discovery, organization, and caching of FFIEC bulk data files
"""

import os
import json
import hashlib
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Dict, List, Tuple, Optional, Set
import logging
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

@dataclass
class BulkFileMetadata:
    """Metadata for a bulk data file"""
    filename: str
    filepath: str
    schedule_code: str
    report_date: str
    quarter: int
    year: int
    file_size: int
    file_hash: str
    last_modified: float
    is_processed: bool = False
    processed_date: Optional[str] = None
    row_count: Optional[int] = None
    institution_count: Optional[int] = None
    
class BulkFileManager:
    """
    Manages FFIEC bulk data files with caching, organization, and progress tracking
    """
    
    # Standard FFIEC bulk file patterns
    SCHEDULE_PATTERNS = {
        'GL': r'FFIEC\s+CDR\s+Call\s+Schedule\s+GL\s+(\d{8})',
        'RC': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RC\s+(\d{8})',
        'RCA': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCA\s+(\d{8})',
        'RCB': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCB\s+(\d{8})',
        'RCCI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCCI\s+(\d{8})',
        'RCCII': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCCII\s+(\d{8})',
        'RCD': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCD\s+(\d{8})',
        'RCE': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCE\s+(\d{8})',
        'RCEI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCEI\s+(\d{8})',
        'RCEII': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCEII\s+(\d{8})',
        'RCF': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCF\s+(\d{8})',
        'RCG': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCG\s+(\d{8})',
        'RCH': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCH\s+(\d{8})',
        'RCI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCI\s+(\d{8})',
        'RCK': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCK\s+(\d{8})',
        'RCL': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCL\s+(\d{8})',
        'RCM': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCM\s+(\d{8})',
        'RCN': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCN\s+(\d{8})',
        'RCO': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCO\s+(\d{8})',
        'RCP': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCP\s+(\d{8})',
        'RCQ': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCQ\s+(\d{8})',
        'RCRI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCRI\s+(\d{8})',
        'RCRII': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCRII\s+(\d{8})',
        'RCS': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCS\s+(\d{8})',
        'RCT': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCT\s+(\d{8})',
        'RCV': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RCV\s+(\d{8})',
        'RI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RI\s+(\d{8})',
        'RIA': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RIA\s+(\d{8})',
        'RIBI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RIBI\s+(\d{8})',
        'RIBII': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RIBII\s+(\d{8})',
        'RIC': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RIC\s+(\d{8})',
        'RID': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RID\s+(\d{8})',
        'RIE': r'FFIEC\s+CDR\s+Call\s+Schedule\s+RIE\s+(\d{8})',
        'CI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+CI\s+(\d{8})',
        'ENT': r'FFIEC\s+CDR\s+Call\s+Schedule\s+ENT\s+(\d{8})',
        'GI': r'FFIEC\s+CDR\s+Call\s+Schedule\s+GI\s+(\d{8})',
        'NARR': r'FFIEC\s+CDR\s+Call\s+Schedule\s+NARR\s+(\d{8})',
        'SU': r'FFIEC\s+CDR\s+Call\s+Schedule\s+SU\s+(\d{8})',
        'POR': r'FFIEC\s+CDR\s+Call\s+Bulk\s+POR\s+(\d{8})',
    }
    
   # Expected schedules for a complete quarter
    EXPECTED_SCHEDULES = [
        'GL', 'RC', 'RCA', 'RCB', 'RCCI', 'RCCII', 'RCD', 'RCE', 'RCEI', 'RCEII',
        'RCF', 'RCG', 'RCH', 'RCI', 'RCK', 'RCL', 'RCM', 'RCN', 'RCO', 'RCP', 
        'RCQ', 'RCRI', 'RCRII', 'RCS', 'RCT', 'RCV', 'RI', 'RIA', 'RIBI', 'RIBII',
        'RIC', 'RID', 'RIE', 'CI', 'ENT', 'GI', 'NARR', 'SU', 'POR'
    ]
    
    def __init__(self, cache_dir: str = None, logger=None):
        self.logger = logger or logging.getLogger('FIRE.FileManager')
        self.cache_dir = cache_dir or os.path.join(os.path.expanduser("~"), ".fire_cache")
        self.db_path = os.path.join(self.cache_dir, "bulk_files.db")
        self.progress_callbacks = []
        self.current_progress = {}
        self._lock = threading.Lock()
        
        # Create cache directory
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Initialize database
        self._init_database()
        
    def _init_database(self):
        """Initialize SQLite database for file metadata caching"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS file_metadata (
                    filepath TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    schedule_code TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    quarter INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_hash TEXT NOT NULL,
                    last_modified REAL NOT NULL,
                    is_processed BOOLEAN DEFAULT 0,
                    processed_date TEXT,
                    row_count INTEGER,
                    institution_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    -- New fields for error recovery
                    processing_status TEXT DEFAULT 'pending',  -- pending, processing, completed, failed
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_retry_date TEXT
                )
            ''')
            
            # Create indices for common queries
            conn.execute('CREATE INDEX IF NOT EXISTS idx_schedule ON file_metadata(schedule_code)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_quarter ON file_metadata(year, quarter)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_processed ON file_metadata(is_processed)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_status ON file_metadata(processing_status)')
    
    def scan_directory(self, directory: str, progress_callback=None) -> Dict[str, List[BulkFileMetadata]]:
        """
        Scan directory for FFIEC bulk files and organize by quarter
        
        Args:
            directory: Path to scan
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary mapping "YYYY-Q#" to list of file metadata
        """
        self.logger.info(f"📁 Scanning directory: {directory}")
        
        if progress_callback:
            self.progress_callbacks.append(progress_callback)
        
        # Find all .txt files
        txt_files = list(Path(directory).glob("*.txt"))
        total_files = len(txt_files)
        
        self.logger.info(f"Found {total_files} text files")
        
        discovered_files = []
        processed = 0
        
        # Process files in parallel for speed
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_file = {
                executor.submit(self._analyze_file, str(f)): f 
                for f in txt_files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                processed += 1
                
                try:
                    metadata = future.result()
                    if metadata:
                        discovered_files.append(metadata)
                        self._update_progress('scan', processed, total_files, 
                                            f"Found: {metadata.schedule_code}")
                except Exception as e:
                    self.logger.error(f"Error analyzing {file_path}: {e}")
                
                self._update_progress('scan', processed, total_files)
        
        # Organize by quarter
        quarters = {}
        for file_meta in discovered_files:
            quarter_key = f"{file_meta.year}-Q{file_meta.quarter}"
            if quarter_key not in quarters:
                quarters[quarter_key] = []
            quarters[quarter_key].append(file_meta)
        
        # Cache metadata
        self._cache_metadata(discovered_files)
        
        # Log summary
        for quarter, files in quarters.items():
            self.logger.info(f"  {quarter}: {len(files)} files")
            missing = self._check_missing_schedules(files)
            if missing:
                self.logger.warning(f"    Missing schedules: {', '.join(missing)}")
        
        return quarters
    
    def _analyze_file(self, filepath: str) -> Optional[BulkFileMetadata]:
        """Analyze a single file and extract metadata"""
        filename = os.path.basename(filepath)
        
        # Try to match against known patterns
        for schedule_code, pattern in self.SCHEDULE_PATTERNS.items():
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                report_date = match.group(1)  # MMDDYYYY format
                
                # Parse date
                try:
                    date_obj = datetime.strptime(report_date, '%m%d%Y')
                    quarter = (date_obj.month - 1) // 3 + 1
                    year = date_obj.year
                except:
                    self.logger.warning(f"Could not parse date from: {filename}")
                    continue
                
                # Get file stats
                stat = os.stat(filepath)
                
                # Calculate file hash for change detection
                file_hash = self._calculate_file_hash(filepath)
                
                # Check for parts (some schedules have multiple parts)
                part_match = re.search(r'\((\d+)\s+of\s+(\d+)\)', filename)
                if part_match:
                    part_num = part_match.group(1)
                    schedule_code = f"{schedule_code}{part_num}"
                
                return BulkFileMetadata(
                    filename=filename,
                    filepath=filepath,
                    schedule_code=schedule_code,
                    report_date=report_date,
                    quarter=quarter,
                    year=year,
                    file_size=stat.st_size,
                    file_hash=file_hash,
                    last_modified=stat.st_mtime
                )
        
        return None
    
    def _calculate_file_hash(self, filepath: str, chunk_size: int = 8192) -> str:
        """Calculate SHA256 hash of file (first 1MB for speed)"""
        sha256_hash = hashlib.sha256()
        bytes_read = 0
        max_bytes = 1024 * 1024  # 1MB
        
        with open(filepath, "rb") as f:
            while bytes_read < max_bytes:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                sha256_hash.update(chunk)
                bytes_read += len(chunk)
        
        return sha256_hash.hexdigest()
    
    def _check_missing_schedules(self, files: List[BulkFileMetadata]) -> List[str]:
        """Check which expected schedules are missing"""
        found_schedules = {f.schedule_code for f in files}
        missing = []
        
        for expected in self.EXPECTED_SCHEDULES:
            # Check base schedule and numbered parts
            if expected not in found_schedules:
                # Check if it's a multi-part schedule
                if not any(f.startswith(expected) for f in found_schedules):
                    missing.append(expected)
        
        return missing
    
    def _cache_metadata(self, files: List[BulkFileMetadata]):
        """Cache file metadata to database"""
        with sqlite3.connect(self.db_path) as conn:
            for file_meta in files:
                # Check if file already exists
                existing = conn.execute(
                    'SELECT file_hash, is_processed FROM file_metadata WHERE filepath = ?',
                    (file_meta.filepath,)
                ).fetchone()
                
                if existing:
                    # Update only if file changed
                    if existing[0] != file_meta.file_hash:
                        conn.execute('''
                            UPDATE file_metadata 
                            SET file_hash = ?, last_modified = ?, is_processed = 0,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE filepath = ?
                        ''', (file_meta.file_hash, file_meta.last_modified, file_meta.filepath))
                else:
                    # Insert new record
                    conn.execute('''
                        INSERT INTO file_metadata 
                        (filepath, filename, schedule_code, report_date, quarter, year,
                         file_size, file_hash, last_modified)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        file_meta.filepath, file_meta.filename, file_meta.schedule_code,
                        file_meta.report_date, file_meta.quarter, file_meta.year,
                        file_meta.file_size, file_meta.file_hash, file_meta.last_modified
                    ))
    
    def get_cached_metadata(self, directory: str = None) -> Dict[str, List[BulkFileMetadata]]:
        """Retrieve cached metadata from database"""
        with sqlite3.connect(self.db_path) as conn:
            query = 'SELECT * FROM file_metadata'
            params = []
            
            if directory:
                query += ' WHERE filepath LIKE ?'
                params.append(f'{directory}%')
            
            query += ' ORDER BY year DESC, quarter DESC, schedule_code'
            
            rows = conn.execute(query, params).fetchall()
            
            # Convert to BulkFileMetadata objects
            files = []
            for row in rows:
                files.append(BulkFileMetadata(
                    filepath=row[0],
                    filename=row[1],
                    schedule_code=row[2],
                    report_date=row[3],
                    quarter=row[4],
                    year=row[5],
                    file_size=row[6],
                    file_hash=row[7],
                    last_modified=row[8],
                    is_processed=bool(row[9]),
                    processed_date=row[10],
                    row_count=row[11],
                    institution_count=row[12]
                ))
            
            # Organize by quarter
            quarters = {}
            for file_meta in files:
                quarter_key = f"{file_meta.year}-Q{file_meta.quarter}"
                if quarter_key not in quarters:
                    quarters[quarter_key] = []
                quarters[quarter_key].append(file_meta)
            
            return quarters
    
    def mark_processed(self, filepath: str, row_count: int, institution_count: int):
        """Mark a file as processed and store statistics"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE file_metadata 
                SET is_processed = 1, 
                    processed_date = CURRENT_TIMESTAMP,
                    row_count = ?,
                    institution_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE filepath = ?
            ''', (row_count, institution_count, filepath))
    
    def update_processing_status(self, filepath: str, status: str, error_message: str = None):
        """Update the processing status of a file"""
        with sqlite3.connect(self.db_path) as conn:
            if status == 'failed' and error_message:
                conn.execute('''
                    UPDATE file_metadata 
                    SET processing_status = ?,
                        error_message = ?,
                        retry_count = retry_count + 1,
                        last_retry_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE filepath = ?
                ''', (status, error_message, filepath))
            else:
                conn.execute('''
                    UPDATE file_metadata 
                    SET processing_status = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE filepath = ?
                ''', (status, filepath))

    def get_pending_files(self, quarter: str = None) -> List[BulkFileMetadata]:
        """Get files that haven't been processed yet"""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT * FROM file_metadata 
                WHERE processing_status IN ('pending', 'failed')
            '''
            params = []
            
            if quarter:
                # Parse quarter format "YYYY-Q#"
                year, q = quarter.split('-Q')
                query += ' AND year = ? AND quarter = ?'
                params.extend([int(year), int(q)])
            
            query += ' ORDER BY schedule_code'
            
            rows = conn.execute(query, params).fetchall()
            
            # Convert to BulkFileMetadata objects
            files = []
            for row in rows:
                files.append(BulkFileMetadata(
                    filepath=row[0],
                    filename=row[1],
                    schedule_code=row[2],
                    report_date=row[3],
                    quarter=row[4],
                    year=row[5],
                    file_size=row[6],
                    file_hash=row[7],
                    last_modified=row[8],
                    is_processed=bool(row[9]),
                    processed_date=row[10],
                    row_count=row[11],
                    institution_count=row[12]
                ))
            
            return files

    def get_failed_files(self, quarter: str = None) -> List[Tuple[BulkFileMetadata, str]]:
        """Get files that failed processing with their error messages"""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                SELECT *, error_message FROM file_metadata 
                WHERE processing_status = 'failed'
            '''
            params = []
            
            if quarter:
                year, q = quarter.split('-Q')
                query += ' AND year = ? AND quarter = ?'
                params.extend([int(year), int(q)])
            
            rows = conn.execute(query, params).fetchall()
            
            # Return tuples of (BulkFileMetadata, error_message)
            failed_files = []
            for row in rows:
                file_meta = BulkFileMetadata(
                    filepath=row[0],
                    filename=row[1],
                    schedule_code=row[2],
                    report_date=row[3],
                    quarter=row[4],
                    year=row[5],
                    file_size=row[6],
                    file_hash=row[7],
                    last_modified=row[8],
                    is_processed=False,
                    processed_date=row[10],
                    row_count=row[11],
                    institution_count=row[12]
                )
                error_msg = row[16] if len(row) > 16 else "Unknown error"
                failed_files.append((file_meta, error_msg))
            
            return failed_files

    def reset_failed_files(self, quarter: str = None):
        """Reset failed files to pending status for retry"""
        with sqlite3.connect(self.db_path) as conn:
            query = '''
                UPDATE file_metadata 
                SET processing_status = 'pending',
                    error_message = NULL
                WHERE processing_status = 'failed'
            '''
            params = []
            
            if quarter:
                year, q = quarter.split('-Q')
                query += ' AND year = ? AND quarter = ?'
                params.extend([int(year), int(q)])
            
            conn.execute(query, params)
          
    def get_processing_stats(self) -> Dict:
        """Get overall processing statistics"""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}
            
            # Total files
            stats['total_files'] = conn.execute(
                'SELECT COUNT(*) FROM file_metadata'
            ).fetchone()[0]
            
            # Processed files
            stats['processed_files'] = conn.execute(
                'SELECT COUNT(*) FROM file_metadata WHERE is_processed = 1'
            ).fetchone()[0]
            
            # By quarter
            quarter_stats = conn.execute('''
                SELECT year, quarter, 
                       COUNT(*) as total,
                       SUM(is_processed) as processed
                FROM file_metadata
                GROUP BY year, quarter
                ORDER BY year DESC, quarter DESC
            ''').fetchall()
            
            stats['quarters'] = [
                {
                    'quarter': f"{row[0]}-Q{row[1]}",
                    'total': row[2],
                    'processed': row[3] or 0
                }
                for row in quarter_stats
            ]
            
            return stats
    
    def _update_progress(self, operation: str, current: int, total: int, message: str = ""):
        """Update progress and notify callbacks"""
        with self._lock:
            self.current_progress[operation] = {
                'current': current,
                'total': total,
                'percentage': (current / total * 100) if total > 0 else 0,
                'message': message
            }
            
            for callback in self.progress_callbacks:
                try:
                    callback(self.current_progress)
                except Exception as e:
                    self.logger.error(f"Progress callback error: {e}")
    
    def validate_quarter_completeness(self, quarter_files: List[BulkFileMetadata]) -> Dict:
        """Validate if a quarter has all expected files"""
        found_schedules = {f.schedule_code for f in quarter_files}
        missing = self._check_missing_schedules(quarter_files)
        
        return {
            'is_complete': len(missing) == 0,
            'found_count': len(found_schedules),
            'expected_count': len(self.EXPECTED_SCHEDULES),
            'missing_schedules': missing,
            'completeness_percentage': (len(found_schedules) / len(self.EXPECTED_SCHEDULES)) * 100
        }
    
    def get_available_quarters(self) -> List[str]:
        """Get list of available quarters from cache"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute('''
                SELECT DISTINCT year, quarter 
                FROM file_metadata 
                ORDER BY year DESC, quarter DESC
            ''').fetchall()
            
            return [f"{row[0]}-Q{row[1]}" for row in rows]
    
    def cleanup_cache(self, days_old: int = 90):
        """Remove old processed entries from cache"""
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        with sqlite3.connect(self.db_path) as conn:
            deleted = conn.execute('''
                DELETE FROM file_metadata 
                WHERE is_processed = 1 
                AND processed_date < ?
            ''', (cutoff_date.isoformat(),)).rowcount
            
            self.logger.info(f"Cleaned up {deleted} old cache entries")


class BulkDataOrganizer:
    """
    High-level organizer for bulk data processing workflows
    """
    
    def __init__(self, file_manager: BulkFileManager, processor, logger=None):
        self.file_manager = file_manager
        self.processor = processor
        self.logger = logger or logging.getLogger('FIRE.Organizer')
        self.current_batch = []
        
    def prepare_quarter_batch(self, quarter: str, directory: str) -> List[BulkFileMetadata]:
        """Prepare a batch of files for a specific quarter"""
        # Scan or get from cache
        quarters = self.file_manager.get_cached_metadata(directory)
        
        if not quarters:
            self.logger.info("No cached data found, scanning directory...")
            quarters = self.file_manager.scan_directory(directory)
        
        if quarter not in quarters:
            raise ValueError(f"Quarter {quarter} not found in available data")
        
        files = quarters[quarter]
        
        # Validate completeness
        validation = self.file_manager.validate_quarter_completeness(files)
        if not validation['is_complete']:
            self.logger.warning(
                f"Quarter {quarter} is incomplete: missing {validation['missing_schedules']}"
            )
        
        self.current_batch = sorted(files, key=lambda f: f.schedule_code)
        return self.current_batch
    
    def prepare_quarter_batch_with_resume(self, quarter: str, directory: str, 
                                    retry_failed: bool = False) -> List[BulkFileMetadata]:
        """Prepare a batch of files for a specific quarter with resume capability"""
        # First, scan or get from cache to ensure metadata is current
        quarters = self.file_manager.get_cached_metadata(directory)
        
        if not quarters:
            self.logger.info("No cached data found, scanning directory...")
            quarters = self.file_manager.scan_directory(directory)
        
        if quarter not in quarters:
            raise ValueError(f"Quarter {quarter} not found in available data")
        
        # Get pending files (unprocessed or failed)
        if retry_failed:
            # Reset failed files to pending
            self.file_manager.reset_failed_files(quarter)
            self.logger.info("Reset failed files for retry")
        
        # Get files that need processing
        pending_files = self.file_manager.get_pending_files(quarter)
        
        if not pending_files:
            self.logger.info("All files already processed successfully!")
            return []
        
        # Check for any failed files
        failed_files = self.file_manager.get_failed_files(quarter)
        if failed_files:
            self.logger.warning(f"Found {len(failed_files)} previously failed files:")
            for file_meta, error_msg in failed_files[:5]:  # Show first 5
                self.logger.warning(f"  - {file_meta.schedule_code}: {error_msg}")
            if len(failed_files) > 5:
                self.logger.warning(f"  ... and {len(failed_files) - 5} more")
        
        # Validate completeness of remaining files
        all_files = quarters[quarter]
        processed_count = len(all_files) - len(pending_files)
        
        self.logger.info(f"Quarter {quarter} status:")
        self.logger.info(f"  Total files: {len(all_files)}")
        self.logger.info(f"  Already processed: {processed_count}")
        self.logger.info(f"  Pending: {len(pending_files)}")
        self.logger.info(f"  Failed: {len(failed_files)}")
        
        self.current_batch = sorted(pending_files, key=lambda f: f.schedule_code)
        return self.current_batch
    
    def process_batch(self, files: List[BulkFileMetadata], 
                 target_rssd_id: str = None,
                 progress_callback=None,
                 resume_mode: bool = False) -> Dict:
        """Process a batch of files with progress tracking and error recovery"""
        results = {}
        total_files = len(files)
        failed_files = []
        
        self.logger.info(f"Starting batch processing of {total_files} files")
        if resume_mode:
            self.logger.info("📥 Resume mode: Processing only pending/failed files")
        if target_rssd_id:
            self.logger.info(f"Filtering for RSSD ID: {target_rssd_id}")
        
        for idx, file_meta in enumerate(files):
            try:
                # Update status to processing
                self.file_manager.update_processing_status(file_meta.filepath, 'processing')
                
                # Update progress with detailed schedule information
                if progress_callback:
                    progress_callback({
                        'current_file': idx + 1,
                        'total_files': total_files,
                        'current_schedule': file_meta.schedule_code,
                        'percentage': ((idx + 1) / total_files) * 100,
                        'message': f"Processing {file_meta.schedule_code}: {file_meta.filename}",
                        'schedule_name': self.processor.dictionary.get_schedule_info(file_meta.schedule_code).get('name', file_meta.schedule_code)
                    })
                
                # Check if already processed successfully
                if file_meta.is_processed and not resume_mode:
                    self.logger.info(f"Skipping already processed: {file_meta.schedule_code}")
                    continue
                
                # Process file
                self.logger.debug(f"Processing {file_meta.schedule_code}: {file_meta.filepath}")
                
                df = self.processor.process_bulk_file(
                    file_meta.filepath, 
                    target_rssd_id=target_rssd_id
                )
                
                if not df.empty:
                    results[file_meta.schedule_code] = df
                    self.logger.info(f"✓ {file_meta.schedule_code}: {len(df)} rows extracted")
                    
                    # Update metadata
                    institution_count = df['RSSDID'].nunique() if 'RSSDID' in df.columns else 0
                    self.file_manager.mark_processed(
                        file_meta.filepath,
                        len(df),
                        institution_count
                    )
                    
                    # Update status to completed
                    self.file_manager.update_processing_status(file_meta.filepath, 'completed')
                    
                    # Send completion update for this schedule
                    if progress_callback:
                        progress_callback({
                            'current_file': idx + 1,
                            'total_files': total_files,
                            'current_schedule': file_meta.schedule_code,
                            'percentage': ((idx + 1) / total_files) * 100,
                            'message': f"✓ Completed {file_meta.schedule_code} ({len(df):,} rows)",
                            'schedule_completed': True
                        })
                else:
                    self.logger.warning(f"⚠️ {file_meta.schedule_code}: No data found")
                    # Mark as completed even if no data (not an error)
                    self.file_manager.update_processing_status(file_meta.filepath, 'completed')
                    
            except Exception as e:
                error_msg = f"Error processing {file_meta.schedule_code}: {str(e)}"
                self.logger.error(error_msg)
                import traceback
                self.logger.error(traceback.format_exc())
                
                # Update status to failed
                self.file_manager.update_processing_status(
                    file_meta.filepath, 
                    'failed', 
                    error_message=str(e)
                )
                
                # Track failed file
                failed_files.append({
                    'schedule': file_meta.schedule_code,
                    'filename': file_meta.filename,
                    'error': str(e)
                })
                
                # Send error update
                if progress_callback:
                    progress_callback({
                        'current_file': idx + 1,
                        'total_files': total_files,
                        'current_schedule': file_meta.schedule_code,
                        'percentage': ((idx + 1) / total_files) * 100,
                        'message': f"❌ Failed {file_meta.schedule_code}: {str(e)}",
                        'schedule_failed': True
                    })
                
                # Continue processing other files
                continue
        
        # Send final summary
        if progress_callback:
            success_count = len(results)
            fail_count = len(failed_files)
            
            summary_msg = f"✅ Batch complete: {success_count} schedules processed"
            if fail_count > 0:
                summary_msg += f", {fail_count} failed"
            
            progress_callback({
                'current_file': total_files,
                'total_files': total_files,
                'percentage': 100,
                'message': summary_msg,
                'batch_complete': True,
                'schedules_processed': success_count,
                'failed_files': failed_files
            })
        
        self.logger.info(f"Batch processing complete. Results: {len(results)} schedules with data, {len(failed_files)} failures")
        
        # Return results with failure information
        return {
            'data': results,
            'failed_files': failed_files
        }

    def get_processing_summary(self) -> Dict:
        """Get summary of current processing state"""
        stats = self.file_manager.get_processing_stats()
        
        if self.current_batch:
            batch_processed = sum(1 for f in self.current_batch if f.is_processed)
            stats['current_batch'] = {
                'total': len(self.current_batch),
                'processed': batch_processed,
                'percentage': (batch_processed / len(self.current_batch)) * 100
            }
        
        return stats