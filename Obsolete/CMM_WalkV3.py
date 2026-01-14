import pandas as pd
import re
import os
import urllib
import warnings
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
DB_TABLE_DETAILS = 'CMM_MeasurementDetails'

# Database Connection
params = urllib.parse.quote_plus(
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=(local)\SQLEXPRESS;' 
    r'DATABASE=QualityShareData;'
    r'Trusted_Connection=yes;'
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def extract_date_from_filename(file_path):
    """
    Parses the date and time from the filename string.
    Example: 20251101321 -> 2025-01-10 13:21
    """
    filename = os.path.basename(file_path)
    match = re.search(r'(\d{10,12})', filename)
    
    if not match:
        return pd.to_datetime(os.path.getctime(file_path), unit='s')

    ds = match.group(1)
    try:
        year = int(ds[:4])
        minute = int(ds[-2:])
        mid = ds[4:-2]
        
        if len(mid) == 4:
            month, day, hour = int(mid[0]), int(mid[1:3]), int(mid[3])
        elif len(mid) == 5:
            month, day, hour = int(mid[0]), int(mid[1:3]), int(mid[3:5])
        elif len(mid) == 6:
            month, day, hour = int(mid[0:2]), int(mid[2:4]), int(mid[4:6])
        else:
            return pd.to_datetime(os.path.getctime(file_path), unit='s')

        return pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute)
    except Exception:
        return pd.to_datetime(os.path.getctime(file_path), unit='s')

def parse_asc_measurements(file_path):
    summary = {"Max_Dev": None, "Status": "Unknown"}
    rows = []
    file_date = extract_date_from_filename(file_path)

    try:
        with open(file_path, 'r', errors='ignore') as f:
            lines = f.readlines()

        max_devs = []
        for idx, line in enumerate(lines, start=1):
            parts = [p.strip() for p in line.split(';')]
            if not any(parts): continue

            if any('OUT' in (p or '').upper() for p in parts):
                summary['Status'] = 'FAIL'

            def to_num(s):
                if not s: return None
                s2 = re.sub(r"[^0-9eE+\-\.]", '', s)
                try: return float(s2)
                except: return None

            row = {
                'RowIndex': idx,
                'Code': parts[0] if len(parts) > 0 else None,
                'Feature': parts[1] if len(parts) > 1 else None,
                'MeasurementType': parts[2] if len(parts) > 2 else None,
                'Nominal': to_num(parts[3]) if len(parts) > 3 else None,
                'UpperTol': to_num(parts[4]) if len(parts) > 4 else None,
                'LowerTol': to_num(parts[5]) if len(parts) > 5 else None,
                'Measured': to_num(parts[6]) if len(parts) > 6 else None,
                'Deviation': to_num(parts[7]) if len(parts) > 7 else None,
                'Flag': parts[8] if len(parts) > 8 else None,
                'Extra': parts[9] if len(parts) > 9 else None,
                'FileName': os.path.basename(file_path),
                'FullPath': file_path,
                'CreatedDate': file_date
            }
            
            dev = abs(row['Deviation']) if row['Deviation'] is not None else None
            if dev is not None: max_devs.append(dev)
            rows.append(row)

        if max_devs:
            summary['Max_Dev'] = max(max_devs)
            if summary['Status'] == 'Unknown': summary['Status'] = 'PASS'

    except Exception as e:
        print(f"Error parsing {file_path}: {e}")

    return {'Summary': summary, 'Rows': rows}

def extract_metadata_from_path(full_path):
    model = "Unknown"
    if "High Capacity (967)" in full_path: model = "967K"
    elif "Mid Capacity ( 031 )" in full_path: model = "031C"
    elif "T324" in full_path: model = "T324"
    
    line_no = "N/A"
    line_match = re.search(r'(LINE|L)\s*(\d+)', full_path, re.IGNORECASE)
    if line_match: line_no = line_match.group(2)
    
    process = "N/A"
    process_match = re.search(r'(#(?:1[0-9]0|200|[1-9]0|80LL)|MQC)', full_path)
    if process_match: process = process_match.group(1)
            
    shift, piece = "N/A", "N/A"
    if "1st" in full_path.lower(): shift = "1st"
    elif "3rd" in full_path.lower(): shift = "3rd"
        
    piece_match = re.search(r'[\\ ]([13])(ATC|BTC|TC|F|M|L)', full_path)
    if piece_match:
        piece = piece_match.group(2)
        if shift == "N/A": shift = "1st" if piece_match.group(1) == "1" else "3rd"
    
    machine_no = "N/A"
    if "10A" in full_path: machine_no = "10A"
    elif "10B" in full_path: machine_no = "10B"

    meas_result = parse_asc_measurements(full_path)
    summary = meas_result.get('Summary', {})
    
    return {
        "CreatedDate": extract_date_from_filename(full_path),
        "Model": model,
        "LineNumber": line_no, # Matches SQL [LineNumber]
        "Process": process,
        "Shift": shift,
        "Piece": piece,
        "MachineNumber": machine_no, # Matches SQL [MachineNumber]
        "Max_Dev": summary.get("Max_Dev"),
        "Status": summary.get("Status"),
        "FileName": os.path.basename(full_path),
        "FullPath": full_path,
        "Measurements": meas_result.get('Rows', [])
    }

def main():
    # 1. Check for table existence
    if not inspect(engine).has_table(DB_TABLE):
        print(f"Error: Table {DB_TABLE} not found. Please run the SQL script first.")
        return

    # 2. Identify already processed files
    query = f"SELECT FullPath FROM {DB_TABLE}"
    existing_paths = set(pd.read_sql(query, engine)['FullPath'])
    print(f"Checking database... {len(existing_paths)} files already processed.")

    all_summary_data = []
    all_details_rows = []

    # 3. Scan directory
    print(f"Scanning {ROOT_DIRECTORY}...")
    for root, _, files in os.walk(ROOT_DIRECTORY):
        for file in files:
            if file.lower().endswith(".asc"):
                path = os.path.join(root, file)
                if path in existing_paths: continue
                
                try:
                    data = extract_metadata_from_path(path)
                    # Separate the summary from the list of measurement rows
                    summary_entry = {k: v for k, v in data.items() if k != 'Measurements'}
                    all_summary_data.append(summary_entry)
                    
                    if data.get('Measurements'):
                        all_details_rows.extend(data.get('Measurements'))
                except Exception as e:
                    print(f"Error processing {file}: {e}")

    if not all_summary_data:
        print("No new files found to upload.")
        return

    # 4. Upload Summary Data
    try:
        df_summary = pd.DataFrame(all_summary_data)
        df_summary.to_sql(DB_TABLE, engine, if_exists='append', index=False)
        print(f"Successfully uploaded {len(df_summary)} new file summaries.")
    except Exception as e:
        print(f"Database error (Summary): {e}")

    # 5. Upload Detail Data
    if all_details_rows:
        try:
            df_details = pd.DataFrame(all_details_rows)
            df_details.to_sql(DB_TABLE_DETAILS, engine, if_exists='append', index=False)
            print(f"Successfully uploaded {len(df_details)} measurement detail rows.")
        except Exception as e:
            print(f"Database error (Details): {e}")

if __name__ == "__main__":
    main()