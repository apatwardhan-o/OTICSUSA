import pandas as pd
import re
import os
import urllib
import warnings
from datetime import datetime
from sqlalchemy import create_engine, inspect

# --- SILENCE WARNINGS ---
warnings.filterwarnings("ignore", category=UserWarning, module='sqlalchemy')
try:
    from sqlalchemy import exc as sa_exc
    warnings.filterwarnings("ignore", category=sa_exc.SAWarning)
except ImportError:
    pass

# --- CONFIGURATION ---
ROOT_DIRECTORY = r'C:\Users\User\OneDrive - oticsusa.com\Lab_Data\Rear Cover'
DB_TABLE = 'CMM_Measurements'

# Database Connection - ODBC Driver 18
params = urllib.parse.quote_plus(
    r'DRIVER={ODBC Driver 18 for SQL Server};'
    r'SERVER=(local)\SQLEXPRESS;' 
    r'DATABASE=QualityShareData;'
    r'Trusted_Connection=yes;'
    r'TrustServerCertificate=yes;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def extract_date_from_filename(file_path):
    """
    Parses the date and time from the filename string.
    Priority:
    1. Regex match in filename (e.g., 20251101321 -> 2025-01-10 13:21)
    2. OS File Creation Date (fallback)
    """
    filename = os.path.basename(file_path)
    # Look for a sequence of 10 to 12 digits
    match = re.search(r'(\d{10,12})', filename)
    
    # Fallback to OS time if no regex match
    if not match:
        return pd.to_datetime(os.path.getctime(file_path), unit='s')

    ds = match.group(1)
    try:
        year = int(ds[:4])
        # Default minute/seconds if string is short
        minute = int(ds[-2:])
        mid = ds[4:-2]
        
        # Handle variable length month/day formatting
        if len(mid) == 4:
            month, day, hour = int(mid[0]), int(mid[1:3]), int(mid[3])
        elif len(mid) == 5:
            month, day, hour = int(mid[0]), int(mid[1:3]), int(mid[3:5])
        elif len(mid) == 6:
            month, day, hour = int(mid[0:2]), int(mid[2:4]), int(mid[4:6])
        else:
            # Fallback if structure is weird
            return pd.to_datetime(os.path.getctime(file_path), unit='s')

        return pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute)
    except Exception:
        # Fallback if parsing crashes
        return pd.to_datetime(os.path.getctime(file_path), unit='s')

def parse_asc_measurements(file_path):
    """
    Parses semicolon-delimited (.asc) files and returns a list of dictionaries.
    """
    rows = []
    try:
        with open(file_path, 'r', errors='ignore') as f:
            lines = f.readlines()

        for line in lines:
            parts = [p.strip() for p in line.split(';')]
            
            # Skip empty lines or header artifacts
            if not any(parts) or (len(parts) > 0 and parts[0] == '1' and not parts[1]):
                continue

            def to_num(s):
                if s is None or s == '': return None
                s2 = re.sub(r"[^0-9eE+\-\.]", '', s)
                try: return float(s2)
                except: return None

            pos_no   = parts[0] if len(parts) > 0 else None
            item     = parts[1] if len(parts) > 1 else None
            element  = parts[2] if len(parts) > 2 else None
            nominal  = to_num(parts[3]) if len(parts) > 3 else None
            ul_val   = to_num(parts[4]) if len(parts) > 4 else None
            
            # LL NULL HANDLING: Default to 0.0 if missing
            ll_val_raw = to_num(parts[5]) if len(parts) > 5 else 0.0
            ll_val = ll_val_raw if ll_val_raw is not None else 0.0
            
            actual   = to_num(parts[6]) if len(parts) > 6 else None
            deviation = to_num(parts[7]) if len(parts) > 7 else None
            bar      = parts[8] if len(parts) > 8 else None

            upper_limit = (nominal + ul_val) if (nominal is not None and ul_val is not None) else None
            lower_limit = (nominal + ll_val) if (nominal is not None) else None

            row = {
                'PosNo': pos_no,
                'Item': item,
                'Element': element,
                'Nominal': nominal,
                'UL': ul_val,
                'LL': ll_val,
                'UpperLimit': upper_limit,
                'LowerLimit': lower_limit,
                'Actual': actual,
                'Deviation': deviation,
                'Bar': bar
            }
            rows.append(row)
    except Exception as e:
        print(f"Error parsing {os.path.basename(file_path)}: {e}")
    return rows

def extract_metadata_from_path(full_path):
    """
    Extracts file-level metadata using regex on path and filename.
    Includes smart date extraction from filename.
    """
    path_up = full_path.upper()
    
    # --- UPDATED DATE LOGIC ---
    # Uses the helper function to prioritize filename dates over OS dates
    file_created_at = extract_date_from_filename(full_path)

    # Model Detection
    model = "Unknown"
    if "967" in path_up: model = "967K"
    elif "031" in path_up: model = "031C"
    elif "T324" in path_up: model = "T324"
    
    # Line# 
    line_no = ""
    line_match = re.search(r'(LINE|L)\s*(\d+)', full_path, re.IGNORECASE)
    if line_match: line_no = line_match.group(2)
    
    # ProcessNo
    process = "N/A"
    process_match = re.search(r'(#(?:1[0-9]0|200|[1-9]0|80LL)|MQC)', path_up)
    if process_match: process = process_match.group(1)
            
    # QShift
    shift = ""
    if "1ST" in path_up: shift = "1"
    elif "3RD" in path_up: shift = "3"
        
    # Piece
    piece = "N/A"
    piece_match = re.search(r'[\\ ]([13])(ATC|BTC|TC|F|M|L)', full_path)
    if piece_match:
        piece = piece_match.group(2)
        if shift == "": shift = piece_match.group(1)
    
    # Cavity
    cavity = ""
    cav_match = re.search(r'Cavity-([\w\d]+)', os.path.basename(full_path), re.IGNORECASE)
    if cav_match: cavity = cav_match.group(1)[:3] 

    return {
        "PartType": "Rear Cover" if "Rear Cover" in full_path else "Unknown",
        "Model": model,
        "FilePath": full_path,
        "FileName": os.path.basename(full_path),
        "FileCreatedAt": file_created_at,  # Populated by new logic
        "Line#": line_no,
        "QShift": shift,
        "Piece": piece,
        "ProcessNo": process,
        "Cavity": cavity if cavity else "N/A"
    }

def main():
    existing_paths = set()
    if inspect(engine).has_table(DB_TABLE):
        query = f"SELECT DISTINCT FilePath FROM {DB_TABLE}"
        existing_paths = set(pd.read_sql(query, engine)['FilePath'])
        print(f"Connected to DB. {len(existing_paths)} existing files found.")

    all_rows_to_upload = []
    print(f"Scanning {ROOT_DIRECTORY}...")
    
    for root, dirs, files in os.walk(ROOT_DIRECTORY):
        for file in files:
            if file.lower().endswith(".asc"):
                full_path = os.path.join(root, file)
                if full_path in existing_paths: continue
                
                try:
                    file_meta = extract_metadata_from_path(full_path)
                    measurements = parse_asc_measurements(full_path)
                    
                    # Merge metadata into every measurement row
                    for m in measurements:
                        all_rows_to_upload.append({**file_meta, **m})
                except Exception as e:
                    print(f"Error processing {file}: {e}")

    if not all_rows_to_upload:
        print("No new data.")
        return

    df = pd.DataFrame(all_rows_to_upload)
    
    # Column ordering to match SQL
    sql_cols = [
        'PartType', 'Model', 'FilePath', 'FileName', 'FileCreatedAt', 'Line#', 'QShift', 'Piece', 
        'ProcessNo', 'Cavity', 'PosNo', 'Item', 'Element', 'Nominal', 
        'UpperLimit', 'LowerLimit', 'Actual', 'Deviation', 'Bar', 'UL', 'LL'
    ]
    
    final_cols = [c for c in sql_cols if c in df.columns]
    df = df[final_cols]

    print(f"Uploading {len(df)} rows...")
    try:
        df.to_sql(DB_TABLE, engine, if_exists='append', index=False, chunksize=10000)
        print("Upload successful.")
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    main()