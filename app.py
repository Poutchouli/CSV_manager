import os
from flask import Flask, request, render_template, redirect, url_for, session, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd
import uuid
import json 

app = Flask(__name__)
app.secret_key = 'your_super_secret_key_here' # Needed for Flask sessions (CHANGE THIS IN PRODUCTION!)

# Configure upload folder and allowed extensions
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024 # 50 MB limit

# Create the upload folder if it doesn't exist
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Global storage for processed and raw DataFrames.
# IMPORTANT: In a real multi-user application, use Flask sessions or a database
# to store user-specific data, not global variables. This is for demonstration.
temp_processed_files = {} # Stores {session_id: {file_key: filepath_to_cleaned_csv}}
temp_raw_files_storage = {} # Stores {session_id: {file_key: filepath_to_raw_csv}}


def allowed_file(filename):
    """Checks if the uploaded file has an allowed extension."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def clean_dataframe(df):
    """
    Performs data cleaning operations on a Pandas DataFrame.
    1. Removes leading/trailing spaces from all string columns.
    2. Removes duplicate rows.
    3. Flags potentially inconsistent data (basic example: numerical outliers).
    """
    print("Starting data cleaning...")

    # 1. Remove leading/trailing spaces from string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    print("Removed leading/trailing spaces.")

    # 2. Remove duplicate rows
    initial_rows = len(df)
    df.drop_duplicates(inplace=True)
    duplicates_removed = initial_rows - len(df)
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate row(s).")
    else:
        print("No duplicate rows found.")

    # 3. Flag inconsistent data for manual review
    df['inconsistency_flag'] = False # Initialize 'inconsistency_flag' ONCE at the beginning

    numerical_cols = df.select_dtypes(include=['number']).columns
    for col in numerical_cols:
        if df[col].std() > 0: # Avoid division by zero if std dev is 0
            z_scores = (df[col] - df[col].mean()) / df[col].std()
            # Use | (OR) to accumulate flags: if it was already True, keep it True
            df.loc[z_scores.abs() > 3, 'inconsistency_flag'] = True
            print(f"Flagged potential numerical inconsistencies in column: '{col}'")

    if len(df.columns) >= 2:
        critical_cols_to_check = df.columns[:2].tolist() # Check first two columns
        # Use | (OR) to accumulate flags
        df.loc[df[critical_cols_to_check].isnull().any(axis=1), 'inconsistency_flag'] = True
        print(f"Flagged rows with missing values in critical columns: {critical_cols_to_check}")

    inconsistent_data_count = df['inconsistency_flag'].sum()
    if inconsistent_data_count > 0:
        print(f"Flagged {inconsistent_data_count} row(s) for manual review due to potential inconsistency.")
    else:
        print("No significant inconsistencies flagged.")

    print("Data cleaning complete.")
    return df

@app.route('/')
def index():
    """Renders the main upload page."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handles file uploads and saves raw files temporarily."""
    session_id = str(uuid.uuid4())
    session['session_id'] = session_id # Store in Flask session
    temp_raw_files_storage[session_id] = {} # Initialize raw file storage for this session

    raw_uploaded_file_paths = {} # To temporarily hold paths for this request

    # Process File 1
    file1 = request.files.get('file1')
    if file1 and file1.filename and allowed_file(file1.filename):
        try:
            filename1 = secure_filename(file1.filename)
            temp_raw_filepath1 = os.path.join(app.config['UPLOAD_FOLDER'], f"raw_csv_1_{session_id}_{filename1}")
            file1.save(temp_raw_filepath1) # Save the raw file
            raw_uploaded_file_paths['file1'] = temp_raw_filepath1
            session['file1_original_name'] = filename1 # Store original name
            print(f"Raw File 1 saved: {temp_raw_filepath1}")
        except Exception as e:
            print(f"Error saving File 1: {e}")
            return f"Error saving File 1: {e}", 500

    # Process File 2 (optional)
    file2 = request.files.get('file2')
    if file2 and file2.filename and allowed_file(file2.filename):
        try:
            filename2 = secure_filename(file2.filename)
            temp_raw_filepath2 = os.path.join(app.config['UPLOAD_FOLDER'], f"raw_csv_2_{session_id}_{filename2}")
            file2.save(temp_raw_filepath2) # Save the raw file
            raw_uploaded_file_paths['file2'] = temp_raw_filepath2
            session['file2_original_name'] = filename2 # Store original name
            print(f"Raw File 2 saved: {temp_raw_filepath2}")
        except Exception as e:
            print(f"Error saving File 2: {e}")
            return f"Error saving File 2: {e}", 500

    if not raw_uploaded_file_paths:
        return "No valid CSV files uploaded.", 400
    else:
        # Store raw file paths in the dedicated raw storage for the session
        temp_raw_files_storage[session_id] = raw_uploaded_file_paths
        session['skipped_lines_info'] = {} # Reset this, will be populated after user config
        
        # Redirect to the new configuration page, passing the session_id
        return redirect(url_for('configure_import', session_id=session_id))


@app.route('/configure_import/<session_id>')
def configure_import(session_id):
    """
    Renders the CSV import configuration page.
    """
    # Retrieve raw file paths from our dedicated storage
    raw_file_paths = temp_raw_files_storage.get(session_id)
    if not raw_file_paths:
        return "No raw files to configure. Please upload files first.", 404

    files_to_configure = []
    if 'file1' in raw_file_paths:
        files_to_configure.append({
            'key': 'file1',
            'name': session.get('file1_original_name', 'File 1'),
            'path': raw_file_paths['file1']
        })
    if 'file2' in raw_file_paths:
        files_to_configure.append({
            'key': 'file2',
            'name': session.get('file2_original_name', 'File 2'),
            'path': raw_file_paths['file2']
        })

    # Default parsing options (can be sent to client for default selections)
    default_options = {
        'encoding': 'utf-8',
        'delimiter': ';',  # Changed to semicolon to match your CSV format initially
        'quotechar': '"',
        'has_header': True,
        'skip_rows': 0
    }

    return render_template('configure_import.html',
                           session_id=session_id,
                           files=files_to_configure,
                           default_options=default_options)


@app.route('/preview_csv', methods=['POST'])
def preview_csv():
    """
    API endpoint to generate a CSV preview based on user-selected options.
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data provided"}), 400
        
    session_id = data.get('session_id')
    file_key = data.get('file_key')
    
    # Parsing options from frontend
    encoding = data.get('encoding', 'utf-8')
    delimiter = data.get('delimiter', ',')
    quotechar = data.get('quotechar', '"')
    has_header = data.get('has_header', True)
    skip_rows = int(data.get('skip_rows', 0))

    raw_file_paths = temp_raw_files_storage.get(session_id)
    if not raw_file_paths or file_key not in raw_file_paths:
        return jsonify({"error": "File not found in session for preview."}), 404

    file_path = raw_file_paths[file_key]

    try:
        header_param = 0 if has_header else None
        
        preview_df = pd.read_csv(
            file_path,
            encoding=encoding,
            sep=delimiter,
            quotechar=quotechar,
            doublequote=True, # Assuming standard CSV double-quote escaping
            header=header_param,
            skiprows=skip_rows,
            nrows=20, # Limit to 20 rows for preview
            on_bad_lines='warn', # Still warn in console if lines are malformed even with options
            engine='python'
        )

        # Handle potential numerical headers if header=None
        if not has_header:
            preview_df.columns = [f'Column {i+1}' for i in range(len(preview_df.columns))]

        preview_data = {
            'headers': preview_df.columns.tolist(),
            'rows': preview_df.values.tolist()
        }
        return jsonify(preview_data)

    except Exception as e:
        print(f"Error generating preview for {file_path}: {e}")
        return jsonify({"error": f"Could not generate preview: {e}. Check options."}), 500


@app.route('/final_import/<session_id>', methods=['POST']) # Changed to POST to receive options
def final_import(session_id):
    """
    Performs final import and cleaning based on user-selected options.
    """
    raw_file_paths = temp_raw_files_storage.get(session_id)
    if not raw_file_paths:
        return "No raw files for final import. Please upload files first.", 404

    import_configs = request.get_json() # Get the JSON payload with file-specific options
    if not import_configs:
        return jsonify({"error": "No import configurations provided"}), 400

    final_cleaned_dfs = {}
    skipped_lines_info = {}

    for file_key, raw_file_path in raw_file_paths.items():
        # Get the specific configuration for this file, or fall back to defaults
        config = import_configs.get(file_key, {
            'encoding': 'utf-8',
            'delimiter': ',',
            'quotechar': '"',
            'has_header': True,
            'skip_rows': 0
        })

        try:
            header_param = 0 if config['has_header'] else None
            
            df = pd.read_csv(
                raw_file_path,
                encoding=config['encoding'],
                sep=config['delimiter'],
                quotechar=config['quotechar'],
                doublequote=True,
                header=header_param,
                skiprows=config['skip_rows'],
                on_bad_lines='warn',
                engine='python'
            )
            
            if not config['has_header']:
                df.columns = [f'Column {i+1}' for i in range(len(df.columns))]

            cleaned_df = clean_dataframe(df.copy()) # Perform data cleaning
            
            # Save the cleaned DataFrame to temp_processed_files for display
            temp_filename = f"cleaned_csv_{file_key}_{session_id}.csv"
            temp_filepath = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
            cleaned_df.to_csv(temp_filepath, index=False)
            temp_processed_files.setdefault(session_id, {})[file_key] = temp_filepath
            
            print(f"Final processing and cleaning complete for {file_key}: {temp_filepath}")

        except Exception as e:
            print(f"Error during final import/cleaning for {file_key}: {e}")
            skipped_lines_info[file_key] = f"Error during final import: {e}"
            # This means this specific file failed, but others might still succeed.

    if not temp_processed_files.get(session_id): # Check if any files were successfully processed
        return "No files could be processed and cleaned. Please review your import options.", 500
    else:
        session['skipped_lines_info'] = skipped_lines_info 
        # Clean up raw files after final processing
        for path in raw_file_paths.values():
            if os.path.exists(path):
                os.remove(path)
        temp_raw_files_storage.pop(session_id, None) # Remove raw storage for this session

        return redirect(url_for('display_csv', session_id=session_id))


@app.route('/update_csv_data', methods=['POST'])
def update_csv_data():
    """
    API endpoint to receive and apply data manipulation changes (edit, add/delete row/column).
    """
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No JSON data provided"}), 400

    session_id = data.get('session_id')
    file_key = data.get('file_key')
    changes = data.get('changes') # List of {'type': 'edit'/'add_row'/'delete_row'/'add_col'/'delete_col', ...}

    if session_id not in temp_processed_files or file_key not in temp_processed_files[session_id]:
        return jsonify({"status": "error", "message": "File not found or session expired."}), 404

    file_path = temp_processed_files[session_id][file_key]
    
    try:
        df = pd.read_csv(file_path)

        for change in changes:
            change_type = change['type']
            if change_type == 'edit':
                row_index = int(change['row_index'])
                col_name = change['col_name']
                new_value = change['new_value']
                # Ensure index is within bounds and column exists
                if row_index < len(df) and col_name in df.columns:
                    # Pandas 'at' is efficient for single-value updates by label
                    df.at[row_index, col_name] = new_value
                else:
                    print(f"Warning: Edit out of bounds or column not found: Row {row_index}, Col '{col_name}'. Skipping.")
                    # In a real app, you might want to return a specific error for this change
                    
            elif change_type == 'delete_row':
                # Convert DataTables' internal index (0-based) to actual DataFrame index
                # DataTables' index is for currently displayed data, not necessarily original DataFrame index
                # Since we reset_index after deletions, the DataTables index should align.
                row_indices_to_delete = [idx for idx in change['row_indices'] if idx < len(df)]
                
                # df.drop expects actual index labels. If DF was reset_index, they are 0-based integers.
                df.drop(row_indices_to_delete, inplace=True, errors='ignore') 
                df.reset_index(drop=True, inplace=True) # Reset index after dropping rows for consistency

            elif change_type == 'add_row':
                # Add a new row filled with NaN or empty strings
                new_row_df = pd.DataFrame([['' for _ in df.columns]], columns=df.columns)
                df = pd.concat([df, new_row_df], ignore_index=True)

            elif change_type == 'delete_column':
                col_name = change['col_name']
                if col_name in df.columns:
                    df.drop(columns=[col_name], inplace=True)
                else:
                    print(f"Warning: Column '{col_name}' not found for deletion. Skipping.")

            elif change_type == 'add_column':
                new_col_name = change.get('new_col_name', f'NewColumn_{uuid.uuid4().hex[:4]}') # Generate unique name
                # Ensure the new column name is not already present to avoid overwriting existing data
                if new_col_name not in df.columns:
                    df[new_col_name] = '' # Add new column, filled with empty strings
                else:
                    print(f"Warning: Column '{new_col_name}' already exists. Not adding.")
                    # You might return an error or generate a truly unique name if conflict happens
            else:
                print(f"Warning: Unknown change type: {change_type}")

        # Save the updated DataFrame back to the temporary CSV file
        df.to_csv(file_path, index=False)
        return jsonify({"status": "success", "message": "CSV data updated."})

    except Exception as e:
        print(f"Error updating CSV data: {e}")
        return jsonify({"status": "error", "message": f"Failed to update data: {e}"}), 500


@app.route('/get_column_counts', methods=['POST'])
def get_column_counts():
    """
    API endpoint to get value counts for a specified column.
    """
    data = request.get_json()
    if not data:
        return jsonify({"status": "error", "message": "No JSON data provided"}), 400

    session_id = data.get('session_id')
    file_key = data.get('file_key')
    column_name = data.get('column_name')

    if session_id not in temp_processed_files or file_key not in temp_processed_files[session_id]:
        return jsonify({"status": "error", "message": "File not found or session expired."}), 404

    file_path = temp_processed_files[session_id][file_key]

    try:
        df = pd.read_csv(file_path)

        if column_name not in df.columns:
            return jsonify({"status": "error", "message": f"Column '{column_name}' not found."}), 400

        # Calculate value counts
        # .to_frame() converts Series to DataFrame, .reset_index() makes count a column
        counts_df = df[column_name].value_counts().reset_index()
        counts_df.columns = ['Value', 'Count'] # Rename columns for clarity

        return jsonify({
            "status": "success",
            "column_name": column_name,
            "counts": counts_df.to_dict(orient='records') # Return as list of dicts
        })

    except Exception as e:
        print(f"Error getting column counts: {e}")
        return jsonify({"status": "error", "message": f"Failed to get counts: {e}"}), 500


@app.route('/download_csv/<session_id>/<file_key>/<path:original_filename>')
def download_csv(session_id, file_key, original_filename):
    """
    Allows user to download the processed CSV file.
    """
    if session_id not in temp_processed_files or file_key not in temp_processed_files[session_id]:
        return "File not found for download or session expired.", 404

    file_path = temp_processed_files[session_id][file_key]
    
    if not os.path.exists(file_path):
        return "File does not exist on server for download.", 404

    # The 'as_attachment=True' forces the browser to download the file.
    # 'mimetype' ensures the browser knows it's a CSV.
    # 'download_name' sets the filename the user will see.
    return send_file(file_path, as_attachment=True, mimetype='text/csv', download_name=original_filename)


@app.route('/display_csv/<session_id>')
def display_csv(session_id):
    """
    Displays the cleaned CSV data.
    Retrieves the DataFrame from temporary storage based on session_id.
    """
    if session_id not in temp_processed_files or not temp_processed_files[session_id]:
        return "No processed CSV data found for this session.", 404

    dfs_to_display = {}
    original_names = {}
    
    skipped_lines_info = session.pop('skipped_lines_info', {})

    if 'file1' in temp_processed_files[session_id]:
        file1_path = temp_processed_files[session_id]['file1']
        dfs_to_display['file1'] = pd.read_csv(file1_path)
        original_names['file1_name'] = session.get('file1_original_name', 'File 1')

    if 'file2' in temp_processed_files[session_id]:
        file2_path = temp_processed_files[session_id]['file2']
        dfs_to_display['file2'] = pd.read_csv(file2_path)
        original_names['file2_name'] = session.get('file2_original_name', 'File 2')

    html_tables = {}
    for key, df in dfs_to_display.items():
        columns = df.columns.tolist()
        html_tables[key] = {
            'headers': columns,
            'data': df.values.tolist()
        }

    return render_template('display_csv.html',
                           tables=html_tables,
                           original_names=original_names,
                           session_id=session_id,
                           skipped_lines_info=skipped_lines_info)

if __name__ == '__main__':
    # Get port from environment variable, default to 17653
    import os
    port = int(os.environ.get('FLASK_RUN_PORT', 17653))
    app.run(host='0.0.0.0', port=port, debug=True)