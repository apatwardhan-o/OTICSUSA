import os
import re
from datetime import datetime

import pdfplumber
import pyodbc

# --- CONFIGURATION ---

ROOT_PATH = r"C:\Users\User\OneDrive - oticsusa.com\Lab_Data\Cam Housing\2.4L CH\Surfcom\12-Dec"

DB_CONFIG = {
    "server": r"(local)\SQLEXPRESS",
    "database": "QualityShareData",
}

LOG_FILE = "log_surfcom.txt"


def log_message(message: str) -> None:
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")


def parse_report_date(page) -> datetime.date | None:
    """
    Look for 'Date' followed by a yyyy/mm/dd or yyyymmdd number on the page.
    Returns a date object or None if not found.
    """
    text = page.extract_text() or ""
    # Surfcom example: 'Date' then '2025/12/30'
    m = re.search(r"Date\s+(\d{4})[/-]?(\d{2})[/-]?(\d{2})", text)
    if not m:
        return None
    y, mth, d = m.groups()
    try:
        return datetime(int(y), int(mth), int(d)).date()
    except ValueError:
        return None


def process_cam_housing_assy() -> None:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        "Trusted_Connection=yes;"
    )

    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()

    MODELS = [
        "2.4L CH",
        "A25 CH Gas",
        "A25 CH Hybrid",
        "M20 CH",
        "2GR KAI CH",
        "V6T LH CH",
        "V6T RH CH Gas",
        "V6T LH CH Hybrid",
    ]
    SUBFOLDERS = ["ASSY", "LINE 1", "LINE 2", "LINE 3", "LINE 4", "LINE 5"]

    new_count = 0
    print("Processing... (Updates every 100 files)")

    try:
        for root, dirs, files in os.walk(ROOT_PATH):
            path_up = root.upper()
            current_sub = next((s for s in SUBFOLDERS if s in path_up), "Other")

            for file in files:
                if not file.lower().endswith(".pdf"):
                    continue

                full_path = os.path.join(root, file)

                try:
                    # --- 1. Filename metadata ---
                    file_up = file.upper()
                    if "EX" in file_up:
                        prefix = "EX "
                    elif "IN" in file_up:
                        prefix = "IN "
                    else:
                        prefix = ""

                    op_initials = file.split(".")[0][-2:].strip().upper()
                    found_model = next((m for m in MODELS if m.upper() in path_up), "Unknown")

                    with pdfplumber.open(full_path) as pdf:
                        page = pdf.pages[0]

                        # --- 2. Report date from header ---
                        report_date = parse_report_date(page)
                        if report_date is None:
                            report_date = datetime.fromtimestamp(
                                os.path.getmtime(full_path)
                            ).date()

                        # --- 3. Line-based journal / Ra parsing ---
                        text = page.extract_text() or ""
                        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

                        current_j_num = None   # boxed journal number
                        spec_value = None      # current spec for this group
                        pending_label = None   # Ramax or Ra(1)...Ra(5) waiting for value

                        for i, line in enumerate(lines):
                            # Journal number line (boxed integer, e.g. "6", "5", "4"...)
                            if line.isdigit():
                                current_j_num = line
                                continue

                            # Spec line: "Spec" then number may be on same or following lines
                            if line.lower().startswith("spec"):
                                spec_value = None
                                # search this and the next few lines for a float like 0.63
                                for look in lines[i : i + 4]:
                                    m = re.search(r"(\d+\.\d+)", look)
                                    if m:
                                        spec_value = float(m.group(1))
                                        break
                                continue

                            # Label line only: "Ramax" or "Ra(1)" ... "Ra(5)"
                            if re.fullmatch(r"Ramax|Ra\(\d\)", line, flags=re.IGNORECASE):
                                pending_label = line
                                continue

                            # If there is a pending label, try to treat this line as its value
                            if pending_label is not None:
                                m = re.search(r"([0-9]+\.[0-9]+)", line)
                                if m and current_j_num is not None:
                                    value = float(m.group(1))
                                    label = pending_label
                                    pending_label = None

                                    final_journal = f"{prefix}Journal {current_j_num}".strip()

                                    cursor.execute(
                                        """
                                        INSERT INTO Surfcom_CamHousing_Assy
                                            (part_model,
                                             sub_folder,
                                             file_date,
                                             journal_no,
                                             measured_item,
                                             measured_value,
                                             spec,
                                             operator_initials,
                                             full_file_path)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (
                                            found_model,
                                            current_sub,
                                            report_date,
                                            final_journal,
                                            label,
                                            value,
                                            spec_value,
                                            op_initials,
                                            full_path,
                                        ),
                                    )

                                    new_count += 1
                                    if new_count % 100 == 0:
                                        print(
                                            f"[{datetime.now().strftime('%H:%M:%S')}] "
                                            f"Processed {new_count} rows..."
                                        )
                                # whether matched or not, continue loop
                                continue

                except Exception as e:
                    log_message(f"Error {file}: {e}")

    finally:
        conn.close()

    print(f"\nFINISHED: Imported {new_count} rows.")
    input("Press Enter to exit...")


if __name__ == "__main__":
    process_cam_housing_assy()
