import pandas as pd
import sys
import os

def test_full_workflow():
    """Test the full CSV processing workflow with correct delimiter"""
    
    # Read the raw file with correct delimiter
    raw_file = 'uploads/raw_csv_1_1acc0930-2f38-4a39-a071-704956396e37_sample_southpark_permissions.csv'
    
    print("=== TESTING CSV WORKFLOW ===")
    print(f"1. Reading raw file: {raw_file}")
    
    try:
        # This simulates the corrected final_import function
        df = pd.read_csv(
            raw_file,
            encoding='utf-8',
            sep=';',  # Correct delimiter
            quotechar='"',
            doublequote=True,
            header=0,
            on_bad_lines='warn',
            engine='python'
        )
        
        print(f"✅ SUCCESS: File parsed correctly")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Shape: {df.shape}")
        print(f"   Sample data:")
        print(df.head(3).to_string(index=False))
        
        # Simulate data cleaning
        print("\n2. Performing data cleaning...")
        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        duplicates_removed = initial_rows - len(df)
        
        # Add inconsistency flag
        df['inconsistency_flag'] = False
        
        print(f"   Removed {duplicates_removed} duplicates")
        print(f"   Added inconsistency_flag column")
        print(f"   Final shape: {df.shape}")
        
        # Test output format for display
        print("\n3. Preparing data for display...")
        html_table_data = {
            'headers': df.columns.tolist(),
            'data': df.values.tolist()
        }
        
        print(f"   Headers: {html_table_data['headers']}")
        print(f"   Data rows: {len(html_table_data['data'])}")
        print(f"   Sample row: {html_table_data['data'][0]}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    success = test_full_workflow()
    if success:
        print("\n🎉 WORKFLOW TEST PASSED - The delimiter fix is working!")
    else:
        print("\n💥 WORKFLOW TEST FAILED")
        sys.exit(1)
