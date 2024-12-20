import gspread
from google.oauth2.service_account import Credentials
import csv

# Path to your service account key file
SERVICE_ACCOUNT_FILE = 'tpds-445105-aafa6b454353.json'

# Scopes: For Google Sheets read-only access
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Create credentials object
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Connect to Google Sheets using gspread
gc = gspread.authorize(creds)

# Your Google Sheet ID (replace with your actual ID)
SHEET_ID = '1TU4xxs1PeGvZv6NH9g0uPv4Io8oLTum276cyccsska8'

# Open the sheet by ID, this opens the first worksheet by default
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)  # 0 = first sheet/tab

# Get all the data (list of lists)
data = worksheet.get_all_values()

# data[0] is the header row, data[1:] are the actual rows
header = data[0]
rows = data[1:]


CSV_FILENAME = 'subscriptions_data.csv'

with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

print(f"CSV file '{CSV_FILENAME}' created successfully.")


from google.cloud import storage

# Initialize the GCS client with the same credentials
storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

#GCS bucket name
BUCKET_NAME = 'tpdsbucket'

bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(CSV_FILENAME)

# Upload the file
blob.upload_from_filename(CSV_FILENAME)

print(f"File {CSV_FILENAME} uploaded to GCS bucket {BUCKET_NAME}.")
