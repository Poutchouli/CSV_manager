import os
import shutil
import time
from flask import Flask, request, render_template, redirect, url_for, session, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd
import uuid

app = Flask(__name__)
# IMPORTANT: Change this secret key in a production environment!
app.secret_key = os.urandom(24)

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
SESSION_LIFETIME_SECONDS = 3600  # 1 hour
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

# --- Core Routes ---

@app.route('/')
def index():
    """Renders the main upload page."""
    # Create a new session on visiting the home page
    session.clear()
    session['session_id'] = str(uuid.uuid4())
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handles file uploads and saves them to the session directory."""
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
        # Handle case where no valid files were uploaded
        return redirect(url_for('index')) # Or show an error

    return redirect(url_for('display_data'))

@app.route('/display')
def display_data():
    """Displays the uploaded CSV data in editable tables."""
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
                print(f"Error reading {filepath}: {e}")
                # Optionally, pass error info to the template
                tables[file_key] = {'error': str(e), 'original_name': session.get(f'{file_key}_original_name')}


    return render_template('display_csv.html', tables=tables)

@app.route('/download/<file_key>')
def download_csv(file_key):
    """Downloads the specified CSV file from the session."""
    filepath = get_filepath(file_key)
    if not filepath or not os.path.exists(filepath):
        return "File not found or session expired.", 404
        
    download_name = session.get(f'{file_key}_original_name', f'{file_key}.csv')
    return send_file(filepath, as_attachment=True, download_name=download_name)

# --- API Endpoints for Data Manipulation ---

@app.route('/api/update_data', methods=['POST'])
def update_data():
    """Applies a list of changes to a CSV file."""
    data = request.get_json()
    file_key = data.get('file_key')
    changes = data.get('changes', [])
    filepath = get_filepath(file_key)

    if not filepath or not os.path.exists(filepath):
        return jsonify({"status": "error", "message": "File not found."}), 404

    try:
        df = pd.read_csv(filepath)
        reload_required = False
        
        # Process deletions first to avoid index shifting issues
        delete_row_indices = [c['row_index'] for c in changes if c['type'] == 'delete_row']
        if delete_row_indices:
            df = df.drop(index=delete_row_indices).reset_index(drop=True)

        delete_col_names = [c['col_name'] for c in changes if c['type'] == 'delete_column']
        if delete_col_names:
            df = df.drop(columns=delete_col_names, errors='ignore')
            reload_required = True

        # Process other changes
        for change in changes:
            change_type = change['type']
            if change_type == 'edit':
                # Assumes indices from non-deleted rows are still valid after reset_index
                row_idx, col_name, new_val = change['row_index'], change['col_name'], change['new_value']
                if row_idx < len(df) and col_name in df.columns:
                    df.loc[row_idx, col_name] = new_val
            elif change_type == 'add_row':
                new_row = pd.DataFrame([['' for _ in df.columns]], columns=df.columns)
                df = pd.concat([df, new_row], ignore_index=True)
            elif change_type == 'add_column':
                col_name = change.get('col_name', 'New_Column')
                if col_name not in df.columns:
                    df[col_name] = ''
                    reload_required = True
            elif change_type == 'find_replace':
                find_val, replace_val, col_name = change['find'], change['replace'], change.get('col_name')
                if col_name and col_name in df.columns:
                    df[col_name] = df[col_name].astype(str).replace(find_val, replace_val)
                else: # All columns
                    df = df.replace(find_val, replace_val)


        df.to_csv(filepath, index=False)
        return jsonify({"status": "success", "message": "Changes saved!", "reload_required": reload_required})

    except Exception as e:
        print(f"Error updating data: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/get_column_summary', methods=['POST'])
def get_column_summary():
    """Generates a summary for a column (for charts)."""
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
        # Histogram for numerical data
        counts, bins = pd.np.histogram(col_data, bins=10)
        labels = [f"{bins[i]:.1f}-{bins[i+1]:.1f}" for i in range(len(bins)-1)]
        return jsonify({"status": "success", "type": "histogram", "labels": labels, "data": counts.tolist()})
    else:
        # Bar chart for categorical or low-cardinality numerical data
        counts = col_data.astype(str).value_counts().nlargest(20)
        return jsonify({"status": "success", "type": "bar", "labels": counts.index.tolist(), "data": counts.values.tolist()})

@app.route('/api/merge_tables', methods=['POST'])
def merge_tables():
    """Merges two tables and saves the result as a new file."""
    data = request.get_json()
    key1, key2 = data.get('file_key1'), data.get('file_key2')
    col1, col2 = data.get('column1'), data.get('column2')
    how = data.get('how', 'inner')

    path1, path2 = get_filepath(key1), get_filepath(key2)
    if not all([path1, path2, os.path.exists(path1), os.path.exists(path2)]):
        return jsonify({"status": "error", "message": "One or both files not found."}), 404

    try:
        df1, df2 = pd.read_csv(path1), pd.read_csv(path2)
        merged_df = pd.merge(df1, df2, left_on=col1, right_on=col2, how=how, suffixes=('_1', '_2'))
        
        # Save merged file
        session_dir = get_session_dir()
        merged_key = f"file{len([k for k in session if k.endswith('_filename')]) + 1}"
        merged_filename = f"merged_{session.get(f'{key1}_original_name')}_{session.get(f'{key2}_original_name')}.csv"
        merged_filepath = os.path.join(session_dir, secure_filename(merged_filename))
        
        merged_df.to_csv(merged_filepath, index=False)

        session[f'{merged_key}_filename'] = secure_filename(merged_filename)
        session[f'{merged_key}_original_name'] = merged_filename

        return jsonify({"status": "success", "message": "Tables merged successfully!"})
    except Exception as e:
        print(f"Merge error: {e}")
        return jsonify({"status": "error", "message": f"Merge failed: {e}"}), 500
        
@app.route('/api/compare_tables', methods=['POST'])
def compare_tables():
    """Compares two tables on a key column and shows rows that are not in both."""
    data = request.get_json()
    key1, key2 = data.get('file_key1'), data.get('file_key2')
    col1, col2 = data.get('column1'), data.get('column2')

    path1, path2 = get_filepath(key1), get_filepath(key2)
    if not all([path1, path2, os.path.exists(path1), os.path.exists(path2)]):
        return jsonify({"status": "error", "message": "One or both files not found."}), 404

    try:
        df1, df2 = pd.read_csv(path1), pd.read_csv(path2)
        
        df1[col1] = df1[col1].astype(str)
        df2[col2] = df2[col2].astype(str)

        comparison_df = pd.merge(df1, df2, left_on=col1, right_on=col2, how='outer', indicator=True, suffixes=('_1', '_2'))

        name1 = session.get(f'{key1}_original_name', 'Table 1')
        name2 = session.get(f'{key2}_original_name', 'Table 2')
        
        comparison_df['comparison_status'] = comparison_df['_merge'].replace({
            'left_only': f'Unique to {name1}',
            'right_only': f'Unique to {name2}',
            'both': 'In Both Tables'
        })
        
        diff_df = comparison_df[comparison_df['_merge'] != 'both'].copy()
        
        if not diff_df.empty:
            cols = diff_df.columns.tolist()
            cols.insert(0, cols.pop(cols.index('comparison_status')))
            diff_df = diff_df[cols]
        
        session_dir = get_session_dir()
        comp_key = f"file{len([k for k in session if k.endswith('_filename')]) + 1}"
        comp_filename = f"comparison_{session.get(f'{key1}_original_name')}_vs_{session.get(f'{key2}_original_name')}.csv"
        comp_filepath = os.path.join(session_dir, secure_filename(comp_filename))
        
        diff_df.to_csv(comp_filepath, index=False)

        session[f'{comp_key}_filename'] = secure_filename(comp_filename)
        session[f'{comp_key}_original_name'] = comp_filename

        return jsonify({"status": "success", "message": "Comparison complete! New table created with differences."})
    except Exception as e:
        print(f"Comparison error: {e}")
        return jsonify({"status": "error", "message": f"Comparison failed: {e}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=17653, debug=True)
