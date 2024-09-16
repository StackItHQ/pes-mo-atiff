import os.path
import time
from decimal import Decimal
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import mysql.connector

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1XdkL4RMX6Cg1AfWn6eJkyKXWAEgBm4mmRCal9dog_w0"
REFRESH_INTERVAL = 15

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="FIListin&*(789",
    database="company"
)
cursor = db.cursor()

def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def create_table_if_not_exists():
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        ID INT PRIMARY KEY,
        NAME VARCHAR(255),
        ROLE VARCHAR(255),
        SALARY_USD DECIMAL(10, 2),
        LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)
    db.commit()

def update_row_in_db(row):
    sql = """
    INSERT INTO employees (ID, NAME, ROLE, SALARY_USD) 
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        NAME = VALUES(NAME),
        ROLE = VALUES(ROLE),
        SALARY_USD = VALUES(SALARY_USD)
    """
    try:
        if len(row) < 4:
            print(f"Skipping row due to insufficient data: {row}")
            return

        id_val = int(row[0]) if row[0] and row[0].strip().isdigit() else None
        name_val = row[1] if len(row) > 1 else None
        role_val = row[2] if len(row) > 2 else None
        salary_val = Decimal(row[3]) if len(row) > 3 and row[3] and row[3].strip().replace('.', '', 1).isdigit() else None

        if id_val is None:
            print(f"Skipping row due to invalid ID: {row}")
            return

        cursor.execute(sql, (id_val, name_val, role_val, salary_val))
        db.commit()
        print(f"Updated/Added row in DB: {row}")
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        db.rollback()
    except Exception as e:
        print(f"Unexpected error when processing row {row}: {e}")
        db.rollback()

def delete_row_from_db(id):
    sql = "DELETE FROM employees WHERE ID = %s"
    try:
        cursor.execute(sql, (id,))
        db.commit()
        print(f"Deleted row from DB with ID: {id}")
    except mysql.connector.Error as err:
        print(f"MySQL Error when deleting row {id}: {err}")
        db.rollback()
    except Exception as e:
        print(f"Unexpected error when deleting row {id}: {e}")
        db.rollback()

def get_credentials():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds

def fetch_spreadsheet_data(service):
    try:
        result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range="Sheet1").execute()
        return result.get("values", [])
    except HttpError as err:
        print(f"An error occurred: {err}")
        return None

def update_spreadsheet(service, values):
    try:
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID, range="Sheet1",
            valueInputOption="RAW", body=body).execute()
        print(f"{result.get('updatedCells')} cells updated in the spreadsheet.")
    except HttpError as err:
        print(f"An error occurred while updating the spreadsheet: {err}")

def fetch_db_data():
    cursor.execute("SELECT ID, NAME, ROLE, SALARY_USD FROM employees")
    rows = cursor.fetchall()
    return [tuple(decimal_to_float(value) for value in row) for row in rows]

def get_last_update_time():
    cursor.execute("SELECT MAX(LAST_UPDATED) FROM employees")
    result = cursor.fetchone()
    return result[0] if result[0] else "1970-01-01 00:00:00"  # Default to epoch time if no data exists

def update_spreadsheet_from_db(service, last_update_time):
    print(f"Last update time: {last_update_time}")
    sql = "SELECT ID, NAME, ROLE, SALARY_USD FROM employees WHERE LAST_UPDATED > %s"
    cursor.execute(sql, (last_update_time,))
    rows = cursor.fetchall()

    if not rows:
        print("No new changes in the database to update in the spreadsheet.")
        return

    # Convert all Decimal values to float before updating the spreadsheet
    values = [list(map(decimal_to_float, row)) for row in rows]
    update_spreadsheet(service, values)
    print("Updated spreadsheet with new changes from the database.")

def compare_and_update(current_sheet_data, last_sheet_data, service):
    if not current_sheet_data:
        print("No data in the current spreadsheet. Skipping update.")
        return

    if last_sheet_data is None:
        for row in current_sheet_data[1:]:  # Skip header row
            update_row_in_db(row)
        print("Initial data loaded into database.")
        return

    current_sheet_ids = set(row[0] for row in current_sheet_data[1:] if row and row[0].strip().isdigit())
    last_sheet_ids = set(row[0] for row in last_sheet_data[1:] if row and row[0].strip().isdigit())

    for row in current_sheet_data[1:]:
        if row and row[0].strip().isdigit():
            if row[0] not in last_sheet_ids or row not in last_sheet_data:
                update_row_in_db(row)

    for row in last_sheet_data[1:]:
        if row and row[0].strip().isdigit() and row[0] not in current_sheet_ids:
            delete_row_from_db(row[0])

    db_data = fetch_db_data()
    db_dict = {str(row[0]): row for row in db_data}
    sheet_dict = {row[0]: row for row in current_sheet_data[1:] if row and row[0].strip().isdigit()}

    updated_sheet_data = [current_sheet_data[0]]  # Keep the header row

    for id, db_row in db_dict.items():
        sheet_row = sheet_dict.get(id)
        if not sheet_row or list(db_row) != sheet_row:
            updated_sheet_data.append([str(value) for value in db_row])
        else:
            updated_sheet_data.append(sheet_row)

    if updated_sheet_data != current_sheet_data:
        update_spreadsheet(service, updated_sheet_data)

def main():
    create_table_if_not_exists()
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    
    last_data = None
    last_update_time = "1970-01-01 00:00:00"  # Initialize with a default timestamp
    
    while True:
        print("Checking for updates in the spreadsheet...")
        current_data = fetch_spreadsheet_data(service)
        
        if current_data is None:
            print("Failed to fetch data. Retrying in 60 seconds...")
            time.sleep(60)
            continue

        compare_and_update(current_data, last_data, service)
        
        print(f"Waiting for {REFRESH_INTERVAL} seconds before checking database for updates...")
        time.sleep(REFRESH_INTERVAL)
        
        update_spreadsheet_from_db(service, last_update_time)
        
        last_update_time = get_last_update_time()
        last_data = current_data

if __name__ == "__main__":
    main()
