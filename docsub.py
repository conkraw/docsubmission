import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore

# --- Firebase Initialization ---
# Convert the AttrDict to a regular dict.
firebase_creds = st.secrets["firebase_service_account"].to_dict()

if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)
db = firestore.client()

# --- Version Determination ---
def determine_version(df):
    """
    Check the DataFrame columns for _v2 or _v1.
    Returns "v2" if any column contains "_v2", otherwise "v1" if any column contains "_v1".
    """
    if any("_v2" in col for col in df.columns):
        return "v2"
    elif any("_v1" in col for col in df.columns):
        return "v1"
    else:
        return None

# --- Firestore Record Check Functions ---
def is_record_processed(record_id, version):
    """Return True if record_id already exists in the version-specific Firestore collection."""
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    return doc_ref.get().exists

def mark_record_as_processed(record_id, version):
    """Mark record_id as processed in the version-specific Firestore collection."""
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    doc_ref.set({"processed": True})

# --- File Processing Function ---
def process_file(uploaded_file):
    df = pd.read_csv(uploaded_file)
    
    # Determine file version (v1 or v2)
    version = determine_version(df)
    if version is None:
        st.error("Could not determine file version (_v1 or _v2 not found in column names).")
        return None, None
    
    # Drop an existing record_id column if present
    if "record_id" in df.columns:
        df = df.drop(columns=["record_id"])
    
    # Dynamically identify an email column (case-insensitive)
    email_column_name = next((col for col in df.columns if "email" in col.lower()), None)
    if email_column_name is None:
        st.error("No email column found in the uploaded file.")
        return None, None

    # Create a 'record_id' column from the email column
    df["record_id"] = df[email_column_name].astype(str)
    
    # Filter out rows whose record_id is already processed in the version-specific collection
    df = df[~df["record_id"].apply(lambda rid: is_record_processed(rid, version))]
    if df.empty:
        st.info("All record_ids in this file have already been processed.")
        return None, version

    # Move 'record_id' to the front
    cols = ["record_id"] + [col for col in df.columns if col != "record_id"]
    df = df[cols]
    
    # Handle timestamp conversion for potential timestamp columns
    timestamp_mapping = {
        "documentation_submission_1_timestamp": "peddoclate1",
        "documentation_submission_2_timestamp": "peddoclate2"
    }
    timestamp_col = None
    for original_col, new_col in timestamp_mapping.items():
        if original_col in df.columns:
            df = df.rename(columns={original_col: new_col})
            timestamp_col = new_col
            # Convert the column to datetime, localize as UTC, then convert to US/Eastern
            df[new_col] = pd.to_datetime(df[new_col], errors="coerce")
            df[new_col] = df[new_col].dt.tz_localize("UTC").dt.tz_convert("US/Eastern")
            df[new_col] = df[new_col].dt.strftime("%m-%d-%Y %H:%M")
    
    # Drop the original email column before output
    df = df.drop(columns=[email_column_name])
    
    # If a timestamp column was processed, remove duplicate record_ids keeping the one with the latest timestamp.
    if timestamp_col:
        df["timestamp_sort"] = pd.to_datetime(df[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce")
        df = df.sort_values(by="timestamp_sort", ascending=False).drop_duplicates(subset=["record_id"], keep="first")
        df = df.drop(columns=["timestamp_sort"])
    
    # Mark each new record_id as processed in the version-specific Firestore collection
    for record_id in df["record_id"]:
        mark_record_as_processed(record_id, version)
    
    return df, version

# --- Streamlit App UI ---
st.title("CSV Processor with Version-Specific Firestore Record Check")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    df_processed, version = process_file(uploaded_file)
    if df_processed is not None:
        st.success("File processed successfully!")
        st.write(f"File version detected: {version}")
        st.dataframe(df_processed)
        
        # Name the output file based on version
        file_name = f"processed_file_{version}.csv"
        
        # Prepare the processed DataFrame for download
        buffer = io.BytesIO()
        df_processed.to_csv(buffer, index=False)
        buffer.seek(0)
        st.download_button(
            label="Download Processed CSV",
            data=buffer,
            file_name=file_name,
            mime="text/csv"
        )
