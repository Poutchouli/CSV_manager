import os
from flask import Flask, request, render_template, redirect, url_for, session, jsonify
from werkzeug.utils import secure_filename
import pandas as pd
import uuid

app = Flask(__name__)
app.secret_key = 'your_super_secret_key_here' # Needed for Flask sessions

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

temp_processed_files = {}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def clean_dataframe(df):
    print("Starting data cleaning...")

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    print("Removed leading/trailing spaces.")

    initial_rows = len(df)
    df.drop_duplicates(inplace=True)
    duplicates_removed = initial_rows - len(df)
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate row(s).")
    else:
        print("No duplicate rows found.")

    df['inconsistency_flag'] = False

    numerical_cols = df.select_dtypes(include=['number']).columns
    for col in numerical_cols:
        if df[col].std() > 0:
            z_scores = (df[col] - df[col].mean()) / df[col].std()
            df.loc[z_scores.abs() > 3, 'inconsistency_flag'] = True
            print(f"Flagged potential numerical inconsistencies in column: '{col}'")

    if len(df.columns) >= 2:
        critical_cols_to_check = df.columns[:2].tolist()
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
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    session_id = str(uuid.uuid4())
    session['session_id'] = session_id
    temp_processed_files[session_id] = {}
    skipped_lines_info = {} # This will be less relevant now, as user configures parsing

    # Store uploaded raw file paths directly
    raw_uploaded_file_paths = {}

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
        # Store raw file paths in session for the next step (config page)
        session['raw_uploaded_file_paths'] = raw_uploaded_file_paths
        session['skipped_lines_info'] = {} # Reset this, will be populated after user config
        
        # Redirect to the new configuration page, passing the session_id
        return redirect(url_for('configure_import', session_id=session_id))

@app.route('/display_csv/<session_id>')
def display_csv(session_id):
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

@app.route('/configure_import/<session_id>')
def configure_import(session_id):
    """
    Renders the CSV import configuration page.
    """
    raw_file_paths = session.get('raw_uploaded_file_paths')
    if not raw_file_paths or session_id not in session.get('session_id', ''):
        return "No raw files to configure. Please upload files first.", 404

    # Prepare data for template
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
        'delimiter': ';',  # Changed to semicolon to match your CSV format
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

    raw_file_paths = session.get('raw_uploaded_file_paths')
    if not raw_file_paths or file_key not in raw_file_paths:
        return jsonify({"error": "File not found in session."}), 404

    file_path = raw_file_paths[file_key]

    try:
        # Read a limited number of rows for preview
        # We use nrows to limit the data loaded, as parsing can be slow for large files
        # Set skiprows if user specifies it
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

        # Convert preview DataFrame to HTML table for display
        # Or, convert to JSON for DataTables / custom rendering if more dynamic JS is used
        # For simplicity, let's send JSON for client-side rendering with simple table
        
        # Also include the inconsistency flag column if it exists,
        # but don't perform full cleaning yet.
        # This preview is just about parsing.
        
        # Return headers and first few rows as JSON
        preview_data = {
            'headers': preview_df.columns.tolist(),
            'rows': preview_df.values.tolist()
        }
        return jsonify(preview_data)

    except Exception as e:
        print(f"Error generating preview for {file_path}: {e}")
        return jsonify({"error": f"Could not generate preview: {e}. Check options."}), 500

@app.route('/final_import/<session_id>')
def final_import(session_id):
    """
    Performs final import, cleaning, and redirects to display.
    Assumes default/last-used parsing options for all files or specific options passed via session/request.
    For simplicity, we'll retrieve default options (or you'd pass them explicitly from configure_import
    if different configs per file were allowed).
    """
    raw_file_paths = session.get('raw_uploaded_file_paths')
    if not raw_file_paths or session_id not in session.get('session_id', ''):
        return "No raw files for final import. Please upload files first.", 404

    # We need to decide how to get the parsing options here.
    # For a multi-file configuration, you'd save the specific options for each
    # file_key (file1, file2) in the session from the 'configure_import' page.
    # For simplicity, let's assume we use the *default* parsing options for now
    # or you'd pass the last configured set here.
    # For a robust solution, consider storing parsed options per file_key in session
    # after they are set on the configure_import page via AJAX, or send them with the POST request.

    # TEMPORARY APPROACH: Use the default options. In a real app,
    # options for each file would be saved from the /configure_import UI.
    # Or, the configure_import page would POST all selected options for all files here.
    
    # Best practice for production:
    # 1. Modify configure_import to POST all file configs to a new /save_configs endpoint.
    # 2. This /final_import route then retrieves those saved configs from the session.
    # For current demonstration, we'll re-apply a common set of parsing rules.

    # Let's take the approach that the last 'preview_csv' call's options are somewhat indicative,
    # but a full-fledged 'Import' button should *POST* the final chosen options.
    # To simplify, we'll just apply standard parsing with 'on_bad_lines' here for now.
    # The true import config should come from the user's choices.

    # Re-using the default parsing options from configure_import for now.
    # In a real app, either store per-file options in session or POST them here.
    parsing_options = {
        'encoding': 'utf-8',
        'delimiter': ';',  # Changed to semicolon to match your CSV format
        'quotechar': '"',
        'has_header': True,
        'skip_rows': 0
    }
    # This is a simplification. For production, you'd have
    # session['file1_parsed_options'] = {...} and session['file2_parsed_options'] = {...}
    # stored by an AJAX call from configure_import when user clicks "Save Config" or similar.

    final_cleaned_dfs = {}
    skipped_lines_info = {}

    for file_key, raw_file_path in raw_file_paths.items():
        try:
            header_param = 0 if parsing_options['has_header'] else None
            
            # Read the full file with chosen (default for now) options
            df = pd.read_csv(
                raw_file_path,
                encoding=parsing_options['encoding'],
                sep=parsing_options['delimiter'],
                quotechar=parsing_options['quotechar'],
                doublequote=True,
                header=header_param,
                skiprows=parsing_options['skip_rows'],
                on_bad_lines='warn',
                engine='python'
            )
            
            # Re-apply column names if header was set to None and original headers exist
            if not parsing_options['has_header']:
                df.columns = [f'Column {i+1}' for i in range(len(df.columns))]

            cleaned_df = clean_dataframe(df.copy()) # Perform data cleaning
            
            # Save the cleaned DataFrame to temp_processed_files for display
            temp_filename = f"cleaned_csv_{file_key}_{session_id}.csv"
            temp_filepath = os.path.join(app.config['UPLOAD_FOLDER'], temp_filename)
            cleaned_df.to_csv(temp_filepath, index=False)
            temp_processed_files[session_id][file_key] = temp_filepath
            
            print(f"Final processing and cleaning complete for {file_key}: {temp_filepath}")

        except Exception as e:
            print(f"Error during final import/cleaning for {file_key}: {e}")
            skipped_lines_info[file_key] = f"Error during final import: {e}"
            # This means this specific file failed, but others might still succeed.

    if not temp_processed_files[session_id]:
        return "No files could be processed and cleaned. Please review your import options.", 500
    else:
        # Pass any final skipped/error info to the display page
        session['skipped_lines_info'] = skipped_lines_info 
        # Clean up raw files after final processing
        for path in raw_file_paths.values():
            if os.path.exists(path):
                os.remove(path)
        session.pop('raw_uploaded_file_paths', None) # Remove from session

        return redirect(url_for('display_csv', session_id=session_id))

if __name__ == '__main__':
    app.run(debug=True)