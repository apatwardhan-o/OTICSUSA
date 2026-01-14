import pdfplumber
import pyodbc
import os
import re
from datetime import datetime

# --- CONFIGURATION ---
ROOT_PATH = r"C:\Users\User\OneDrive - oticsusa.com\Lab_Data\Cam Housing\2.4L CH\Surfcom\12-Dec"
DB_CONFIG = {
    'server': r'(local)\SQLEXPRESS',
    'database': 'QualityShareData'
}

def extract_date_from_filename(file_path):
    """Parses date from filename (YYYYMMDD...) or falls back to OS modification date."""
    filename = os.path.basename(file_path)
    match = re.search(r'(\d{10,12})', filename)
    if not match:
        return datetime.fromtimestamp(os.path.getmtime(file_path))
    
    ds = match.group(1)
    try:
        year, minute = int(ds[:4]), int(ds[-2:])
        mid = ds[4:-2]
        if len(mid) == 4: month, day, hour = int(mid[0]), int(mid[1:3]), int(mid[3])
        elif len(mid) == 5: month, day, hour = int(mid[0]), int(mid[1:3]), int(mid[3:5])
        elif len(mid) == 6: month, day, hour = int(mid[0:2]), int(mid[2:4]), int(mid[4:6])
        return datetime(year=year, month=month, day=day, hour=hour, minute=minute)
    except:
        return datetime.fromtimestamp(os.path.getmtime(file_path))

def process_surfcom():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection=yes;"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # SPEED OPTIMIZATION: Load existing paths into a SET for instant lookup
        print("Loading existing records from database for duplicate checking...")
        cursor.execute("SELECT full_file_path FROM SurfcomMeasurements")
        existing_paths = {row[0] for row in cursor.fetchall() if row[0]}
        print(f"Database ready. Skipping {len(existing_paths)} already imported files.")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # Tracking variables
    new_files_count = 0
    batch_size = 50 # CHANGE THIS: Report and Commit to DB every 50 files
    
    # Model Definitions
    REAR_COVER_MODELS = ['031C', '967K', 'T324']
    CAM_HOUSING_MODELS = ['2.4L CH', 'A25 CH', '2GR KAI CH', 'M20 CH', 'V6T CH']
    
    params_list = ['Ra1max', 'Ra8max', 'Ramax', 'Rz1max', 'Rz8max', 'Rzmax', 'Ra1', 'Ra8', 'Rz1', 'Rz8', 'Ra', 'Rz', 'Rt', 'Pa', 'Pt']
    pdf_pattern = re.compile(r"(" + "|".join(params_list) + r")\s+([\d\.]+)um")
    
    print(f"Scanning Root: {ROOT_PATH}")
    
    # Walk through the entire Lab_Data directory
    for root, dirs, files in os.walk(ROOT_PATH):
        # SPEED CHANGE: Only enter folders that contain 'surfcom'
        if 'surfcom' not in root.lower():
            continue
            
        path_upper = root.upper()
        
        # Determine Part Type
        part_type = "Unknown"
        model_list = []
        if "REAR COVER" in path_upper:
            part_type = "Rear Cover"
            model_list = REAR_COVER_MODELS
        elif "CAM HOUSING" in path_upper:
            part_type = "Cam Housing"
            model_list = CAM_HOUSING_MODELS

        for file in files:
            if file.lower().endswith(".pdf"):
                full_path = os.path.join(root, file)
                
                # DUPLICATE CHECK: Skip files already in DB
                if full_path in existing_paths:
                    continue
                
                try:
                    # Identify Model
                    found_model = "Unknown"
                    for m in model_list:
                        if m.upper() in path_upper or m.upper() in file.upper():
                            found_model = m
                            break

                    file_date = extract_date_from_filename(full_path)

                    # Extract metadata from filename (Process, Item, Initials)
                    tokens = re.findall(r'[a-zA-Z0-9]+', file)
                    if len(tokens) >= 3:
                        proc = tokens[0].upper().replace('P', '').strip()
                        item = tokens[1].strip()
                        init_match = re.search(r'([a-zA-Z]+)', tokens[2])
                        init = init_match.group(0).upper() if init_match else "??"
                    else:
                        proc, item, init = "Unknown", "Unknown", "Unknown"

                    # PDF Extraction
                    with pdfplumber.open(full_path) as pdf:
                        # Only scan top-left area where measurements usually live
                        page = pdf.pages[0]
                        text = page.within_bbox((0, 0, page.width * 0.75, page.height * 0.5)).extract_text()
                        
                        if text:
                            matches = pdf_pattern.findall(text)
                            for param, value in matches:
                                cursor.execute('''
                                    INSERT INTO SurfcomMeasurements 
                                    (part_type, part_model, process_no, item_no, operator_initials, file_date, [Measured Item], [Measured Value], full_file_path)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (part_type, found_model, proc, item, init, file_date, param, float(value), full_path))
                            
                            new_files_count += 1
                            
                            # SPEED & REPORTING CHANGE: Commit and print every 'batch_size'
                            if new_files_count % batch_size == 0:
                                conn.commit()
                                print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {new_files_count} new files...")

                except Exception as e:
                    print(f"Error parsing {file}: {e}")

    # Final commit for the last batch
    conn.commit()
    conn.close()
    print(f"\n--- SUCCESS --- Total New Imports: {new_files_count}")

if __name__ == "__main__":
    process_surfcom()