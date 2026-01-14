import pdfplumber
import pyodbc
import os
import re
from datetime import datetime

# --- CONFIGURATION ---
ROOT_PATH = r"C:\Users\User\OneDrive - oticsusa.com\Lab_Data\Cam Housing"
DB_CONFIG = {
    'server': r'(local)\SQLEXPRESS', 
    'database': 'QualityShareData'
}

def get_metadata_from_path(full_path):
    parts = full_path.split(os.sep)
    part_model, sub_folder = "Unknown", "Unknown"
    for i, p in enumerate(parts):
        if p == "Surfcom" and i > 0: part_model = parts[i-1]
        if any(keyword in p.upper() for keyword in ["ASSY", "LINE", "OP"]): sub_folder = p
    filename = os.path.basename(full_path)
    name_parts = os.path.splitext(filename)[0].split()
    operator_initials = name_parts[-1] if len(name_parts) > 1 else ""
    return part_model, sub_folder, operator_initials

def extract_pdf_data(file_path):
    results = []
    file_date = None
    filename_upper = os.path.basename(file_path).upper()
    
    # Logic: EX files start at Journal 6, IN files start at Journal 5
    if "EX" in filename_upper:
        journal_counter = 6
        prefix = "Exhaust"
    else:
        journal_counter = 5
        prefix = "Intake"

    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                words = page.extract_words(x_tolerance=3, y_tolerance=3)
                if not words: continue
                
                lines = {}
                for w in words:
                    y = round(w['top'], 0)
                    lines.setdefault(y, []).append(w)
                
                sorted_y = sorted(lines.keys())
                for y in sorted_y:
                    line_words = sorted(lines[y], key=lambda x: x['x0'])
                    line_text = " ".join([w['text'] for w in line_words]).strip()

                    # 1. Capture Date
                    date_match = re.search(r"(\d{4}/\d{2}/\d{2})", line_text)
                    if date_match and not file_date:
                        file_date = date_match.group(1)

                    # 2. Measurement Detection
                    label_match = re.search(r"(Ramax|Ra\(\d+\))", line_text)
                    if label_match:
                        item_name = label_match.group(1)
                        
                        # COORDINATE FILTER: Only get numbers to the right of label
                        label_x1 = [w['x1'] for w in line_words if item_name in w['text']][0]
                        measurements = []
                        for w in line_words:
                            if w['x0'] > label_x1:
                                clean_val = w['text'].replace('µm', '').replace('$', '').replace('~', '').strip()
                                try:
                                    val = float(clean_val.replace(',', '.'))
                                    measurements.append(val)
                                except ValueError: continue
                        
                        if measurements:
                            results.append({
                                'journal_no': f"{prefix} Journal {journal_counter}",
                                'measured_item': item_name,
                                'measured_value': measurements[0],
                                'spec': measurements[1] if len(measurements) > 1 else 0.63
                            })
                            
                            # Whenever we finish Ra(5), we know the block is done. Count down.
                            if item_name == "Ra(5)":
                                journal_counter -= 1

    except Exception as e:
        print(f"Error in {os.path.basename(file_path)}: {e}")

    return results, file_date

def run_import():
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting sequence-based scan...")
    files_processed = 0
    
    for root, dirs, files in os.walk(ROOT_PATH):
        folder_upper = root.upper()
        if "ASSY" in folder_upper:
            for file in files:
                file_upper = file.upper()
                if file_upper.endswith(".PDF") and ("EX" in file_upper or "IN" in file_upper):
                    full_path = os.path.join(root, file)
                    
                    cursor.execute("SELECT COUNT(*) FROM Surfcom_CamHousing_Assy WHERE full_file_path = ?", (full_path,))
                    if cursor.fetchone()[0] > 0: continue

                    part_model, sub_folder, initials = get_metadata_from_path(full_path)
                    extracted_rows, pdf_date = extract_pdf_data(full_path)

                    for row in extracted_rows:
                        cursor.execute('''
                            INSERT INTO Surfcom_CamHousing_Assy 
                            (part_model, sub_folder, operator_initials, file_date, journal_no, measured_item, measured_value, spec, full_file_path)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (part_model, sub_folder, initials, pdf_date, row['journal_no'], row['measured_item'], row['measured_value'], row['spec'], full_path))

                    files_processed += 1

    conn.close()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Finished! Total imported: {files_processed}")

if __name__ == "__main__":
    run_import()