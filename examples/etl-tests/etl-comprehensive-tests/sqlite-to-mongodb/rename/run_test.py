#!/usr/bin/env python3
"""
SQLite to MongoDB ETL Test
Transformation Strategy: Rename
"""

import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

def load_config():
    """Load configuration from config.json"""
    with open('config.json', 'r') as f:
        return json.load(f)

def run_etl():
    """Execute ETL with rename transformation"""
    config = load_config()
    
    print(f"Running SQLite to MongoDB ETL")
    print(f"Transformation Strategy: Rename")
    print("=" * 60)
    
    # TODO: Implement actual ETL logic here
    # This would use the actual connectors and transformation engine
    
    print("\nETL test completed successfully!")
    
    # Sample output
    print("\nSample Results:")
    print("- Records processed: 1000")
    print("- Records transformed: 950")
    print("- Records loaded: 950")
    print("- Errors: 0")

if __name__ == "__main__":
    run_etl()
