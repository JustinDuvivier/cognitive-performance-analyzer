"""
Brain Fog Correlation Engine - Main Pipeline Orchestrator

Production-ready pipeline that processes all registered data sources:
1. Read from all registered sources
2. Validate each dataset
3. Clean and normalize data
4. Load to PostgreSQL
5. Log results and rejected records

To add new data sources, register them in data_sources.py
"""

import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_sources import DATA_SOURCES
from validators.validate import validate_batch
from loaders.load import log_rejected_records, check_table_counts

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def format_reject_for_logging(invalid_record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert validator's invalid record format to loader's expected format"""
    return {
        'table': invalid_record.get('table', 'unknown'),
        'record': invalid_record.get('record', {}),
        'error': '; '.join(invalid_record.get('errors', ['Unknown error']))
    }


def process_data_source(source) -> Dict[str, Any]:
    """Process a single data source through the pipeline"""
    stats = {
        'name': source.name,
        'read': 0,
        'validated': 0,
        'rejected': 0,
        'loaded': 0,
        'db_rejected': 0
    }
    
    rejected_records = []
    
    try:
        # Read data
        logger.info(f"Reading from {source.name}...")
        raw_data = source.reader()
        stats['read'] = len(raw_data)
        
        if not raw_data:
            logger.warning(f"No data read from {source.name}")
            return stats
        
        logger.info(f"Read {stats['read']} records from {source.name}")
        
        # Validate data
        valid_data, invalid_data = validate_batch(raw_data, source.validator_table)
        stats['validated'] = len(valid_data)
        stats['rejected'] = len(invalid_data)
        
        if invalid_data:
            rejected_records.extend([format_reject_for_logging(r) for r in invalid_data])
            logger.warning(f"{source.name}: {stats['rejected']} records failed validation")
        
        # Clean data
        cleaned_data = []
        for record in valid_data:
            try:
                cleaned = source.cleaner(record)
                cleaned_data.append(cleaned)
            except Exception as e:
                logger.error(f"Error cleaning record from {source.name}: {e}")
                rejected_records.append({
                    'table': source.table_name,
                    'record': record,
                    'error': f"Cleaning error: {str(e)}"
                })
                stats['rejected'] += 1
        
        # Load data
        if cleaned_data:
            inserted, db_rejected = source.loader(cleaned_data)
            stats['loaded'] = inserted
            stats['db_rejected'] = len(db_rejected)
            
            if db_rejected:
                rejected_records.extend(db_rejected)
                logger.warning(f"{source.name}: {stats['db_rejected']} records failed to load")
            
            logger.info(f"{source.name}: Loaded {stats['loaded']} records to {source.table_name}")
        else:
            logger.warning(f"{source.name}: No valid data to load")
        
    except Exception as e:
        logger.error(f"Error processing {source.name}: {e}", exc_info=True)
        stats['error'] = str(e)
    
    stats['rejected_records'] = rejected_records
    return stats


def run_pipeline(sources: List = None) -> Dict[str, Any]:
    """
    Main pipeline orchestrator
    
    Args:
        sources: Optional list of DataSource objects to process.
                 If None, processes all registered sources.
    
    Returns:
        Dictionary with pipeline statistics
    """
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("BRAIN FOG PIPELINE - Starting ingestion")
    logger.info("=" * 60)
    
    if sources is None:
        sources = DATA_SOURCES
    
    logger.info(f"Processing {len(sources)} data source(s)")
    
    all_stats = []
    all_rejected = []
    
    try:
        # Process each data source
        for source in sources:
            stats = process_data_source(source)
            all_stats.append(stats)
            all_rejected.extend(stats.get('rejected_records', []))
        
        # Log all rejected records
        if all_rejected:
            logger.info("Logging rejected records...")
            logged = log_rejected_records(all_rejected)
            logger.info(f"Logged {logged} rejected records to stg_rejects")
        
        # Calculate summary statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        total_read = sum(s['read'] for s in all_stats)
        total_validated = sum(s['validated'] for s in all_stats)
        total_rejected = sum(s['rejected'] + s.get('db_rejected', 0) for s in all_stats)
        total_loaded = sum(s['loaded'] for s in all_stats)
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"\nTotal Records:")
        logger.info(f"  Read: {total_read}")
        logger.info(f"  Validated: {total_validated}")
        logger.info(f"  Loaded: {total_loaded}")
        logger.info(f"  Rejected: {total_rejected}")
        
        logger.info(f"\nBy Source:")
        for stats in all_stats:
            logger.info(f"  {stats['name']}:")
            logger.info(f"    Read: {stats['read']}, Validated: {stats['validated']}, "
                       f"Loaded: {stats['loaded']}, Rejected: {stats['rejected'] + stats.get('db_rejected', 0)}")
        
        # Show database counts
        counts = check_table_counts()
        if counts:
            logger.info(f"\nDatabase Counts:")
            for table, count in counts.items():
                logger.info(f"  {table}: {count} records")
        
        logger.info("=" * 60)
        logger.info("✅ PIPELINE COMPLETE")
        logger.info("=" * 60)
        
        return {
            'success': True,
            'duration_seconds': duration,
            'sources_processed': len(sources),
            'total_read': total_read,
            'total_validated': total_validated,
            'total_loaded': total_loaded,
            'total_rejected': total_rejected,
            'source_stats': all_stats,
            'start_time': start_time,
            'end_time': end_time
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'duration_seconds': (datetime.now() - start_time).total_seconds(),
            'sources_processed': len(all_stats)
        }


if __name__ == "__main__":
    try:
        result = run_pipeline()
        sys.exit(0 if result.get('success', False) else 1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
