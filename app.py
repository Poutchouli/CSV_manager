import os
import shutil
import time
from flask import Flask, request, render_template, redirect, url_for, session, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
import uuid

app = Flask(__name__)
# IMPORTANT: Change this secret key in a production environment!
app.secret_key = os.urandom(24)

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
SESSION_LIFETIME_SECONDS = 3600  # 1 hour
MAX_MERGE_ROWS = 1_000_000 # Safety limit for merge operations
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB limit

# --- Helper Functions ---

def get_session_dir():
    """Gets the directory for the current session, creating it if it doesn't exist."""
    session_id = session.get('session_id')
    if not session_id:
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
    
    session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
    if not os.path.exists(session_dir):
        os.makedirs(session_dir)
    return session_dir

def get_filepath(file_key):
    """Constructs the full path for a file in the current session."""
    session_dir = get_session_dir()
    filename = session.get(f'{file_key}_filename')
    if not filename:
        return None
    return os.path.join(session_dir, secure_filename(filename))

def allowed_file(filename):
    """Checks if the file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

def cleanup_old_sessions():
    """Deletes session folders older than the defined lifetime."""
    try:
        for session_id in os.listdir(app.config['UPLOAD_FOLDER']):
            session_dir = os.path.join(app.config['UPLOAD_FOLDER'], session_id)
            if os.path.isdir(session_dir):
                if os.path.getmtime(session_dir) < time.time() - SESSION_LIFETIME_SECONDS:
                    shutil.rmtree(session_dir)
                    print(f"Cleaned up old session: {session_id}")
    except Exception as e:
        print(f"Error during session cleanup: {e}")

# --- App Setup ---

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
    
@app.before_request
def before_request_hook():
    """Runs before each request to handle session init and cleanup."""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
    
    # Run cleanup with a 5% chance to avoid overhead on every request
    if uuid.uuid4().int % 100 < 5:
        cleanup_old_sessions()

# --- Core Routes (Unchanged) ---
@app.route('/')
def index():
    session.clear()
    session['session_id'] = str(uuid.uuid4())
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    session_dir = get_session_dir()
    files = request.files.getlist('files[]')
    if not files or files[0].filename == '':
        return redirect(request.url)
    file_count = 0
    for file in files:
        if file and allowed_file(file.filename):
            file_count += 1
            file_key = f'file{file_count}'
            filename = secure_filename(file.filename)
            save_path = os.path.join(session_dir, filename)
            file.save(save_path)
            session[f'{file_key}_filename'] = filename
            session[f'{file_key}_original_name'] = file.filename
    if file_count == 0:
        return redirect(url_for('index'))
    return redirect(url_for('display_data'))

@app.route('/display')
def display_data():
    tables = {}
    file_keys = [key.replace('_filename', '') for key in session if key.endswith('_filename')]
    for file_key in sorted(file_keys):
        filepath = get_filepath(file_key)
        if filepath and os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath, on_bad_lines='warn', engine='python')
                tables[file_key] = {
                    'headers': df.columns.tolist(),
                    'data': df.values.tolist(),
                    'original_name': session.get(f'{file_key}_original_name', 'Unnamed File')
                }
            except Exception as e:
                tables[file_key] = {'error': str(e), 'original_name': session.get(f'{file_key}_original_name')}
    return render_template('display_csv.html', tables=tables)

@app.route('/download/<file_key>')
def download_csv(file_key):
    filepath = get_filepath(file_key)
    if not filepath or not os.path.exists(filepath):
        return "File not found or session expired.", 404
    download_name = session.get(f'{file_key}_original_name', f'{file_key}.csv')
    return send_file(filepath, as_attachment=True, download_name=download_name)

# --- API Endpoints for Data Manipulation ---

@app.route('/api/update_data', methods=['POST'])
def update_data():
    data = request.get_json()
    file_key, changes = data.get('file_key'), data.get('changes', [])
    filepath = get_filepath(file_key)
    if not filepath or not os.path.exists(filepath):
        return jsonify({"status": "error", "message": "File not found."}), 404
    try:
        df = pd.read_csv(filepath)
        # Process deletions first
        delete_row_indices = [c['row_index'] for c in changes if c['type'] == 'delete_row']
        if delete_row_indices:
            df = df.drop(index=delete_row_indices).reset_index(drop=True)
        # Process group deletions
        group_deletions = [c for c in changes if c['type'] == 'delete_group']
        for deletion in group_deletions:
            df[deletion['col_name']] = df[deletion['col_name']].astype(str)
            df = df[df[deletion['col_name']] != str(deletion['group_value'])]
        # Process other changes
        for change in changes:
            if change['type'] == 'edit':
                row_idx, col_name, new_val = change['row_index'], change['col_name'], change['new_value']
                if row_idx < len(df) and col_name in df.columns:
                    df.loc[row_idx, col_name] = new_val
        df.to_csv(filepath, index=False)
        return jsonify({"status": "success", "message": "Changes saved!"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/get_column_summary', methods=['POST'])
def get_column_summary():
    data = request.get_json()
    file_key, col_name = data.get('file_key'), data.get('column_name')
    filepath = get_filepath(file_key)
    if not all([file_key, col_name, filepath, os.path.exists(filepath)]):
        return jsonify({"status": "error", "message": "Invalid request."}), 400
    df = pd.read_csv(filepath)
    if col_name not in df.columns:
        return jsonify({"status": "error", "message": "Column not found."}), 404
    col_data = df[col_name].dropna()
    if pd.api.types.is_numeric_dtype(col_data) and col_data.nunique() > 15:
        counts, bins = np.histogram(col_data, bins=10)
        labels = [f"{bins[i]:.1f}-{bins[i+1]:.1f}" for i in range(len(bins)-1)]
        return jsonify({"status": "success", "type": "histogram", "labels": labels, "data": counts.tolist()})
    else:
        counts = col_data.astype(str).value_counts().nlargest(20)
        return jsonify({"status": "success", "type": "bar", "labels": counts.index.tolist(), "data": counts.values.tolist()})

def _perform_merge_or_compare(data, is_compare=False):
    """Helper function to handle both merge and compare logic."""
    key1, key2 = data.get('file_key1'), data.get('file_key2')
    col1, col2 = data.get('column1'), data.get('column2')
    force = data.get('force', False)
    path1, path2 = get_filepath(key1), get_filepath(key2)

    if not all([path1, path2, os.path.exists(path1), os.path.exists(path2)]):
        return jsonify({"status": "error", "message": "One or both files not found."}), 404

    try:
        df1, df2 = pd.read_csv(path1), pd.read_csv(path2)
        
        # --- Safeguard Logic ---
        if not force:
            # Estimate merge size. This is a simplified estimation.
            # A more accurate one would be more complex, but this handles the worst cases.
            if data.get('how') == 'outer' or is_compare:
                estimated_rows = len(df1) * len(df2)
            else: # inner, left, right
                keys1 = df1[col1].value_counts()
                keys2 = df2[col2].value_counts()
                common_keys = keys1.index.intersection(keys2.index)
                estimated_rows = (keys1[common_keys] * keys2[common_keys]).sum()

            if estimated_rows > MAX_MERGE_ROWS:
                return jsonify({
                    "status": "confirm", 
                    "message": f"This operation is predicted to create approximately {estimated_rows:,} rows, which exceeds the safety limit of {MAX_MERGE_ROWS:,}. This may cause server performance issues. Do you want to proceed?",
                    "action": "confirm_large_operation"
                }), 200 # Use 200 OK for a confirmation request

        # --- Perform Operation ---
        how = 'outer' if is_compare else data.get('how', 'inner')
        merged_df = pd.merge(df1, df2, left_on=col1, right_on=col2, how=how, suffixes=('_1', '_2'), indicator=is_compare)
        
        if is_compare:
            name1 = session.get(f'{key1}_original_name', 'Table 1')
            name2 = session.get(f'{key2}_original_name', 'Table 2')
            merged_df = merged_df[merged_df['_merge'] != 'both'].copy()
            merged_df.drop(columns=['_merge'], inplace=True)
            filename_prefix = "comparison"
        else:
            filename_prefix = "merged"

        # --- Save Result ---
        session_dir = get_session_dir()
        new_key = f"file{len([k for k in session if k.endswith('_filename')]) + 1}"
        new_filename = f"{filename_prefix}_{session.get(f'{key1}_original_name')}_{session.get(f'{key2}_original_name')}.csv"
        new_filepath = os.path.join(session_dir, secure_filename(new_filename))
        merged_df.to_csv(new_filepath, index=False)
        session[f'{new_key}_filename'] = secure_filename(new_filename)
        session[f'{new_key}_original_name'] = new_filename

        return jsonify({"status": "success", "message": "Operation successful!"})

    except Exception as e:
        print(f"Operation error: {e}")
        return jsonify({"status": "error", "message": f"Operation failed: {e}"}), 500

@app.route('/api/merge_tables', methods=['POST'])
def merge_tables():
    """Merges two tables with a safeguard."""
    return _perform_merge_or_compare(request.get_json(), is_compare=False)
        
@app.route('/api/compare_tables', methods=['POST'])
def compare_tables():
    """Compares two tables with a safeguard."""
    return _perform_merge_or_compare(request.get_json(), is_compare=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=17653, debug=True)
