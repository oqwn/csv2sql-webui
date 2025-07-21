#!/usr/bin/env python3
"""
Run all transformation strategies for this ETL combination
"""

import os
import subprocess
import time
from pathlib import Path

TRANSFORMATION_STRATEGIES = [
    "filter", "clean", "aggregate", "join", "split_column",
    "merge_column", "custom_sql", "custom_python", "type_conversion",
    "rename", "drop", "fill_null"
]

def run_transformation(strategy):
    """Run a single transformation test"""
    print(f"\n{'='*60}")
    print(f"Running {strategy.replace('_', ' ').title()} Transformation")
    print('='*60)
    
    strategy_path = Path(strategy)
    if not strategy_path.exists():
        print(f"Skipping {strategy} - directory not found")
        return False
    
    try:
        result = subprocess.run(
            ["python", "run_test.py"],
            cwd=strategy_path,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print(f"✓ {strategy} completed successfully")
            return True
        else:
            print(f"✗ {strategy} failed")
            print(f"Error: {result.stderr}")
            return False
    
    except Exception as e:
        print(f"✗ {strategy} error: {str(e)}")
        return False

def main():
    """Run all transformation tests"""
    print("Running All Transformation Tests")
    print(f"Source → Target: {Path.cwd().name}")
    
    results = {}
    
    for strategy in TRANSFORMATION_STRATEGIES:
        success = run_transformation(strategy)
        results[strategy] = success
        time.sleep(1)  # Brief pause between tests
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print('='*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    print(f"Total: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {total - passed}")
    
    for strategy, success in results.items():
        status = "✓" if success else "✗"
        print(f"{status} {strategy}")

if __name__ == "__main__":
    main()
