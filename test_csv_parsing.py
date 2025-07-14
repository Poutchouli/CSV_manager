import pandas as pd
import os

def test_csv_parsing():
    """Test CSV parsing with different delimiters"""
    
    # Test 1: Comma-separated CSV
    print("Testing comma-separated CSV:")
    try:
        df = pd.read_csv('test_simple.csv')
        print(f"✅ Success: {len(df)} rows, columns: {list(df.columns)}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 2: Semicolon-separated CSV 
    print("\nTesting semicolon-separated CSV:")
    try:
        df = pd.read_csv('uploads/raw_csv_1_1acc0930-2f38-4a39-a071-704956396e37_sample_southpark_permissions.csv', 
                        sep=';')
        print(f"✅ Success: {len(df)} rows, columns: {list(df.columns)}")
        print("First few rows:")
        print(df.head(3))
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 3: Test malformed CSV with default settings
    print("\nTesting malformed CSV with comma delimiter (should have issues):")
    try:
        df = pd.read_csv('uploads/raw_csv_1_1acc0930-2f38-4a39-a071-704956396e37_sample_southpark_permissions.csv', 
                        sep=',', on_bad_lines='skip')
        print(f"✅ Partially successful: {len(df)} rows, columns: {list(df.columns)}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_csv_parsing()
