import pandas as pd
from app import clean_dataframe # Import the function to be tested

def test_remove_spaces():
    """Test if leading/trailing spaces are removed."""
    data = {
        'Col1': [' ValueA ', 'ValueB ', ' ValueC'],
        'Col2': ['  Data1', 'Data2  ', 'Data3']
    }
    df = pd.DataFrame(data)
    cleaned_df = clean_dataframe(df.copy()) # Pass a copy to avoid modifying original test data

    expected_data = {
        'Col1': ['ValueA', 'ValueB', 'ValueC'],
        'Col2': ['Data1', 'Data2', 'Data3'],
        'inconsistency_flag': [False, False, False] # Expected default flags
    }
    expected_df = pd.DataFrame(expected_data)

    # Check if string columns are stripped correctly
    pd.testing.assert_frame_equal(cleaned_df[['Col1', 'Col2']], expected_df[['Col1', 'Col2']])
    # Check if flags are correctly initialized to False (or as expected for this test)
    assert all(cleaned_df['inconsistency_flag'] == False)


def test_remove_duplicates():
    """Test if duplicate rows are removed."""
    data = {
        'ColA': ['X', 'Y', 'X', 'Z'],
        'ColB': [1, 2, 1, 3]
    }
    df = pd.DataFrame(data)
    cleaned_df = clean_dataframe(df.copy())

    expected_data = {
        'ColA': ['X', 'Y', 'Z'],
        'ColB': [1, 2, 3],
        'inconsistency_flag': [False, False, False]
    }
    expected_df = pd.DataFrame(expected_data)

    # We need to compare after sorting to ensure order doesn't fail the test if not intended
    # Or, check shape and content in a more robust way. For simplicity, let's sort.
    # Note: drop_duplicates preserves first occurrence order.
    pd.testing.assert_frame_equal(
        cleaned_df.reset_index(drop=True)[['ColA', 'ColB']],
        expected_df.reset_index(drop=True)[['ColA', 'ColB']]
    )
    assert len(cleaned_df) == 3 # Ensure correct number of rows after removing duplicates
    assert all(cleaned_df['inconsistency_flag'] == False)


def test_flag_numerical_outliers():
    """Test if numerical outliers are flagged."""
    data = {
        'NormalNum': [10, 11, 10, 12, 100], # 100 is an outlier
        'OtherCol': ['A', 'B', 'C', 'D', 'E']
    }
    df = pd.DataFrame(data)
    cleaned_df = clean_dataframe(df.copy())

    # We expect the last row (index 4) to be flagged for 'NormalNum' outlier
    assert cleaned_df.loc[4, 'inconsistency_flag'] == True
    assert all(cleaned_df.loc[[0,1,2,3], 'inconsistency_flag'] == False)


def test_flag_missing_values_critical_cols():
    """Test if rows with missing values in critical columns are flagged."""
    data = {
        'Critical1': ['Val1', 'Val2', None, 'Val4'],
        'Critical2': ['DataA', None, 'DataC', 'DataD'],
        'OtherCol': [1, 2, 3, 4]
    }
    df = pd.DataFrame(data)
    cleaned_df = clean_dataframe(df.copy())

    # Assuming 'Critical1' and 'Critical2' are the first two columns and thus "critical"
    # Row at index 2 (None in Critical1) should be flagged
    # Row at index 1 (None in Critical2) should be flagged
    assert cleaned_df.loc[1, 'inconsistency_flag'] == True # Row with missing Critical2
    assert cleaned_df.loc[2, 'inconsistency_flag'] == True # Row with missing Critical1
    assert all(cleaned_df.loc[[0,3], 'inconsistency_flag'] == False)


def test_no_inconsistencies():
    """Test a dataset with no expected inconsistencies."""
    data = {
        'Col1': ['A', 'B', 'C'],
        'Col2': [1, 2, 3],
        'Col3': ['X', 'Y', 'Z']
    }
    df = pd.DataFrame(data)
    cleaned_df = clean_dataframe(df.copy())
    assert all(cleaned_df['inconsistency_flag'] == False)
    assert len(cleaned_df) == 3 # No duplicates
    pd.testing.assert_frame_equal(
        cleaned_df[['Col1', 'Col2', 'Col3']],
        df[['Col1', 'Col2', 'Col3']] # Should be identical except for flag
    )