import pandas as pd
import openai  # (if needed for further processing)
import firebase_admin
from firebase_admin import credentials, firestore
import json

# Initialize Firebase using credentials from Streamlit secrets (or similar secure storage)
try:
    cred = credentials.Certificate(json.loads(st.secrets["firebase_service_account_json"]))
    firebase_admin.initialize_app(cred)
except Exception:
    # Firebase may already be initialized
    pass

db = firestore.client()

# --- Helper Functions ---

def determine_version(df):
    """
    Determine the version from the DataFrame columns.
    Returns 'v2' if any column ends with '_v2', otherwise 'v1' if any column ends with '_v1'.
    If neither is found, returns None.
    """
    if any(col.endswith('_v2') for col in df.columns):
        return "v2"
    elif any(col.endswith('_v1') for col in df.columns):
        return "v1"
    else:
        return None

def is_record_processed(record_id, version):
    """
    Check in the version-specific collection if a record_id has been processed.
    """
    doc_ref = db.collection(f"processed_records_{version}").document(record_id)
    return doc_ref.get().exists

def mark_record_as_processed(record_id, version):
    """
    Mark a record_id as processed in the version-specific Firebase collection.
    """
    doc_ref = db.collection(f"processed_records_{version}").document(record_id)
    doc_ref.set({"processed": True})

# --- Main Processing Code ---

# Replace FILENAME with your CSV file path
FILENAME = 'PEDIATRICDOCUMENTATI_DATA_2025-02-27_2102.csv'
data = pd.read_csv(FILENAME)

# Determine file version based on column names
version = determine_version(data)
if version is None:
    raise ValueError("No version indicator (_v1 or _v2) found in the columns.")

# Ensure we have a record_id. If missing, try to use an email column.
if "record_id" not in data.columns:
    email_col = next((col for col in data.columns if "email" in col.lower()), None)
    if email_col is None:
        raise ValueError("No record_id or email column found to uniquely identify rows.")
    data["record_id"] = data[email_col].astype(str)

# Filter out rows with record_ids that have already been processed in the version-specific collection.
data = data[~data["record_id"].apply(lambda rid: is_record_processed(rid, version))]
if data.empty:
    print("All record_ids in this file have already been processed. Exiting.")
    exit()

# Define dynamic column names using the version suffix.
hpi_col       = f"historyofpresentillness_{version}"
pmhx_col      = f"pmhx_{version}"
pshx_col      = f"pshx_{version}"
famhx_col     = f"famhx_{version}"
diet_col      = f"diet_{version}"
birthhx_col   = f"birthhx_{version}"
dev_col       = f"dev_{version}"
soc_col       = f"soc_hx_features_{version}"
med_col       = f"med_{version}"
all_col       = f"all_{version}"
imm_col       = f"imm_{version}"

temp_col      = f"temp_{version}"
hr_col        = f"hr_{version}"
rr_col        = f"rr_{version}"
pulseox_col   = f"pulseox_{version}"
sbp_col       = f"sbp_{version}"
dbp_col       = f"dbp_{version}"
weight_col    = f"weight_{version}"
weighttile_col= f"weighttile_{version}"
height_col    = f"height_{version}"
heighttile_col= f"heighttile_{version}"
bmi_col       = f"bmi_{version}"
bmitile_col   = f"bmitile_{version}"

# --- Transformation Steps ---

# Convert the HPI column to string and compute its word count.
data[hpi_col] = data[hpi_col].astype(str)
data[f"hpiwords_{version}"] = data[hpi_col].apply(lambda x: len(x.split()))

# Create the 'additional_hx' column by concatenating various history fields.
data[f"additional_hx_{version}"] = (
    'Past Medical History: ' + data[pmhx_col].fillna('') + '\n' +
    'Past Surgical History: ' + data[pshx_col].fillna('') + '\n' +
    'Family History: ' + data[famhx_col].fillna('') + '\n' +
    'Dietary History: ' + data[diet_col].fillna('') + '\n' +
    'Birth History: ' + data[birthhx_col].fillna('') + '\n' +
    'Developmental History: ' + data[dev_col].fillna('') + '\n' +
    'Social History: ' + data[soc_col].fillna('') + '\n' +
    'Medications: ' + data[med_col].fillna('') + '\n' +
    'Allergies: ' + data[all_col].fillna('') + '\n' + 
    'Immunizations: ' + data[imm_col].fillna('') + '\n'
)

# Create the 'vital_signs_and_growth' column by concatenating various vital signs and growth parameters.
data[f"vital_signs_and_growth_{version}"] = (
    'Temperature: ' + data[temp_col].astype(str) + '\n' +
    'Heart Rate: ' + data[hr_col].astype(str) + '\n' +
    'Respiratory Rate: ' + data[rr_col].astype(str) + '\n' +
    'Pulse Oximetry: ' + data[pulseox_col].astype(str) + '\n' +
    'Systolic Blood Pressure: ' + data[sbp_col].astype(str) + '\n' +
    'Diastolic Blood Pressure: ' + data[dbp_col].astype(str) + '\n' +
    'Weight: ' + data[weight_col].astype(str) + ' (' + data[weighttile_col].astype(str) + ')\n' +
    'Height: ' + data[height_col].astype(str) + ' (' + data[heighttile_col].astype(str) + ')\n' +
    'BMI: ' + data[bmi_col].astype(str) + ' (' + data[bmitile_col].astype(str) + ')'
)

# (Optional) Drop any columns you don't want to keep – for example, the original email column
# if "email" in data.columns:
#     data = data.drop(columns=["email"])

# Mark each processed record_id as processed in Firebase (in the version-specific collection)
for rid in data["record_id"]:
    mark_record_as_processed(rid, version)

# Save the processed data to a new CSV file.
output_filename = f"processed_{FILENAME.split('.')[0]}_{version}.csv"
data.to_csv(output_filename, index=False)
print(f"Processed file saved as {output_filename}")
