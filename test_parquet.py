#!/usr/bin/env python3

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

def test_single_file():
    """Test reading a single parquet file"""
    test_file = Path("../player_data/February_10/0019c582-574d-4a53-9f77-554519b75b4c_1298e3e2-2776-4038-ba9b-72808b041561.nakama-0")
    
    if not test_file.exists():
        print("❌ Test file not found")
        return False
    
    try:
        print("📁 Testing parquet file reading...")
        print(f"File: {test_file}")
        
        # Read with pyarrow
        table = pq.read_table(test_file)
        df = table.to_pandas()
        
        print(f"✅ Successfully read {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Shape: {df.shape}")
        print("\nFirst row:")
        print(df.iloc[0])
        print(f"\nEvent column sample: {df['event'].head()}")
        print(f"Event types: {df['event'].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else str(x)).unique()}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False

if __name__ == "__main__":
    test_single_file()