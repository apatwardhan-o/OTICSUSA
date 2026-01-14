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
        # Match 'LINE 1', 'Line 2', 'L1', 'L2' etc.
        if re.search(r"(LINE\s*\d|L\d)", p, re.IGNORECASE): sub_folder = p
    filename = os.path.basename(full_path)
    name_parts = os.path.splitext(filename)[0].split()
    operator_initials = name_parts[-1] if len(name_parts) > 1 else ""
    return part_model, sub_folder, operator_initials

def extract_line_pdf_data(file_path):
    results = []
    file_date = None
    filename = os.path.basename(file_path).upper()
    
    # Determine the header based on filename
    if "CHAIN CASE EX" or "CH CASE EX" in filename: journal_label = "Chain Case Exhaust"
    elif "CHAIN CASE IN" or "CH CASE IN" in filename: journal_label = "Chain Case Intake"
    elif "HEAD EX" in filename: journal_label = "Head Exhaust"
    elif "HEAD IN" in filename: journal_label = "Head Intake"
    else: journal_label = "Line Measurement"

    # Define the list of items we want to capture
    target_items = ["Pt", "Ra", "Ramax", "Ramin", "Rasd", "Ra(1)", "Ra(2)", "Ra(3)", "Rz(1)", "Rz(2)", "Rz(3)"]

    try:
        with pdfplumber.open(file_path) as pdf:
            page = pdf.pages[0]
            # Use a strict x_tolerance to keep the label and value separate
            words = page.extract_words(x_tolerance=2)
            
            # Group into lines
            lines = {}
            for w in words:
                y = round(w['top'], 0)
                lines.setdefault(y, []).append(w)
            
            for y in sorted(lines.keys()):
                line_words = sorted(lines[y], key=lambda x: x['x0'])
                line_text = " ".join([w['text'] for w in line_words])
                
                # 1. Capture Date
                if not file_date:
                    date_match = re.search(r"(\d{4}/\d{2}/\d{2})", line_text)
                    if date_match: file_date = date_match.group(1)

                # 2. Capture Measurement Data
                # Look for lines that start with our target items
                for item in target_items:
                    # Match item name exactly at the start of the line or in a specific column
                    if line_text.startswith(item + " ") or line_text.startswith(item + "\n"):
                        # The value is usually the next text chunk that looks like a number
                        values = [w['text'] for w in line_words if re.match(r"^\d+\s?\d*\.\d+$", w['text'].replace(' ', ''))]
                        
                        if values:
                            clean_val = values[0].replace(' ', '')
                            results.append({
                                'journal_no': journal_label,
                                'measured_item': item,
                                'measured_value': float(clean_val),
                                'spec': 0.63 # Default spec for Line measurements
                            })
    except Exception as e:
        print(f"Error in {os.path.basename(file_path)}: {e}")

    return results, file_date

def run_import():
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting LINE folder scan...")
    files_processed = 0
    
    for root, dirs, files in os.walk(ROOT_PATH):
        folder_upper = root.upper()
        # Filter for folder containing "LINE" or "L" + digit
        if re.search(r"(LINE\s?\d|L\d)", folder_upper):
            for file in files:
                file_upper = file.upper()
                # Target the four specific file types
                targets = ["CHAIN CASE EX", "CHAIN CASE IN", "HEAD EX", "HEAD IN"]
                if file_upper.endswith(".PDF") and any(t in file_upper for t in targets):
                    full_path = os.path.join(root, file)
                    
                    cursor.execute("SELECT COUNT(*) FROM Surfcom_CamHousing_Assy WHERE full_file_path = ?", (full_path,))
                    if cursor.fetchone()[0] > 0: continue

                    part_model, sub_folder, initials = get_metadata_from_path(full_path)
                    extracted_rows, pdf_date = extract_line_pdf_data(full_path)

                    for row in extracted_rows:
                        cursor.execute('''
                            INSERT INTO Surfcom_CamHousing_Assy 
                            (part_model, sub_folder, operator_initials, file_date, journal_no, measured_item, measured_value, spec, full_file_path)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (part_model, sub_folder, initials, pdf_date, row['journal_no'], row['measured_item'], row['measured_value'], row['spec'], full_path))

                    files_processed += 1
                    print(f"Imported Line Data: {file}")

    conn.close()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Finished! Total Line files imported: {files_processed}")

if __name__ == "__main__":
    run_import()