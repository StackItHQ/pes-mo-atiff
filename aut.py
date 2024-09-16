import os.path
import time
from decimal import Decimal
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import mysql.connector

# Constants
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1XdkL4RMX6Cg1AfWn6eJkyKXWAEgBm4mmRCal9dog_w0"
REFRESH_INTERVAL = 5



def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="FIListin&*(789",
        database="company"
    )



def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj



def create_table_if_not_exists(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        ID INT PRIMARY KEY,
        NAME VARCHAR(255),
        ROLE VARCHAR(255),
        SALARY_USD DECIMAL(10, 2),
        LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """)



def get_credentials():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.expired:
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



def fetch_db_data(cursor):
    cursor.execute("SELECT ID, NAME, ROLE, SALARY_USD FROM employees ORDER BY ID")
    rows = cursor.fetchall()
    return [tuple(decimal_to_float(value) for value in row) for row in rows]



def update_db_from_spreadsheet(cursor, db, spreadsheet_data):
    for row in spreadsheet_data[1:]:  # Skip header row
        sql = """
        INSERT INTO employees (ID, NAME, ROLE, SALARY_USD)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            NAME = VALUES(NAME),
            ROLE = VALUES(ROLE),
            SALARY_USD = VALUES(SALARY_USD)
        """
        try:
            id_val = int(row[0]) if row[0] and row[0].strip().isdigit() else None
            name_val = row[1] if len(row) > 1 else None
            role_val = row[2] if len(row) > 2 else None
            salary_val = Decimal(row[3]) if len(row) > 3 and row[3] and row[3].strip().replace('.', '', 1).isdigit() else None

            if id_val is None:
                print(f"Skipping row due to invalid ID: {row}")
                continue

            cursor.execute(sql, (id_val, name_val, role_val, salary_val))
            db.commit()
            print(f"Updated/Added row in DB: {row}")
        except mysql.connector.Error as err:
            print(f"MySQL Error: {err}")
            db.rollback()
        except Exception as e:
            print(f"Unexpected error when processing row {row}: {e}")
            db.rollback()



def update_spreadsheet_from_db(service, cursor):
    db_data = fetch_db_data(cursor)

    if not db_data:
        print("No data in the database to update in the spreadsheet.")
        return

    values = [["ID", "NAME", "ROLE", "SALARY_USD"]]
    values.extend([[str(value) for value in row] for row in db_data])

    update_spreadsheet(service, values)
    print("Updated spreadsheet with all data from the database.")


def main():
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)

    last_db_data = None
    last_spreadsheet_data = None

    while True:
        print("Checking for updates...")
        # Create a new connection for each iteration beacuse we need to check database updates
        db = get_db_connection()
        cursor = db.cursor(buffered=True)

        create_table_if_not_exists(cursor)
        current_db_data = fetch_db_data(cursor)
        current_spreadsheet_data = fetch_spreadsheet_data(service)

        if current_spreadsheet_data != last_spreadsheet_data:
            print("Spreadsheet changes detected. Updating database...")
            update_db_from_spreadsheet(cursor, db, current_spreadsheet_data)
            last_spreadsheet_data = current_spreadsheet_data
        else:
            print("No changes detected in the spreadsheet.")

        if current_db_data != last_db_data:
            print("Database changes detected. Updating spreadsheet...")
            update_spreadsheet_from_db(service, cursor)
            last_db_data = current_db_data
        else:
            print("No changes detected in the database.")

        cursor.close()
        db.close()

        print(f"Waiting for {REFRESH_INTERVAL} seconds before next check...")
        time.sleep(REFRESH_INTERVAL)

if __name__ == "__main__":
    main()
