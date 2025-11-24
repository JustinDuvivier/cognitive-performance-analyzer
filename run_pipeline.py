import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from ingest import run_pipeline

if __name__ == "__main__":
    try:
        result = run_pipeline()
        if result.get('success', False):
            logging.info("Pipeline completed successfully")
            sys.exit(0)
        else:
            logging.error(f"Pipeline completed with errors: {result.get('error', 'Unknown error')}")
            sys.exit(1)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

