import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import json

# Check if Firebase credentials are available
if "firebase" not in st.secrets or "FIREBASE_COLLECTION_NAME" not in st.secrets:
    st.error("Firebase credentials are missing! Add them to .streamlit/secrets.toml or Streamlit Cloud Secrets Manager.")
    st.stop()

# Load Firebase credentials
firebase_creds = st.secrets["firebase"]

if not firebase_admin._apps:
    cred = credentials.Certificate(json.loads(json.dumps(firebase_creds)))
    firebase_admin.initialize_app(cred)

# Firestore reference
firestore_db = firestore.client()
collection_name = st.secrets["FIREBASE_COLLECTION_NAME"]

# Test Firestore connection
try:
    firestore_db.collection(collection_name).limit(1).get()
    st.success("Connected to Firestore successfully!")
except Exception as e:
    st.error(f"Firestore connection failed: {e}")

# Load existing processed records from Firestore
def load_processed_records():
    docs = firestore_db.collection(collection_name).stream()
    records = {doc.id: doc.to_dict() for doc in docs}
    if records:
        return pd.DataFrame.from_dict(records, orient="index")
    return pd.DataFrame()

# Function to save processed records to Firestore
def save_processed_records(df):
    for _, row in df.iterrows():
        doc_id = row["record_id"]
        firestore_db.collection(collection_name).document(doc_id).set(row.to_dict())

# Load processed records at the start
processed_records = load_processed_records()

# Function to process the uploaded file
def process_file(uploaded_file):
    df = pd.read_csv(uploaded_file)

    # Drop existing record_id column if present
    if "record_id" in df.columns:
        df = df.drop(columns=["record_id"])

    # Identify the email column dynamically
    email_column_name = next((col for col in df.columns if "email" in col.lower()), None)
    
    if email_column_name is None:
        st.error("No email column found in the uploaded file.")
        return None

    # Rename the email column to record_id and ensure it's a string
    df["record_id"] = df[email_column_name].astype(str)

    # Move record_id to the front
    cols = ["record_id"] + [col for col in df.columns if col != "record_id"]
    df = df[cols]

    # Handle timestamp conversion for both possible columns
    timestamp_mapping = {
        "documentation_submission_1_timestamp": "peddoclate1",
        "documentation_submission_2_timestamp": "peddoclate2"
    }

    timestamp_col = None  # Track which timestamp column is used

    for original_col, new_col in timestamp_mapping.items():
        if original_col in df.columns:
            # Rename the column
            df = df.rename(columns={original_col: new_col})
            timestamp_col = new_col  # Store the selected timestamp column

            # Convert to datetime
            df[new_col] = pd.to_datetime(df[new_col], errors="coerce")

            # Convert from UTC to Eastern Time
            df[new_col] = df[new_col].dt.tz_localize("UTC").dt.tz_convert("US/Eastern")

            # Format the datetime
            df[new_col] = df[new_col].dt.strftime("%m-%d-%Y %H:%M")

    # Drop the original email column before downloading
    df = df.drop(columns=[email_column_name])

    # Identify new records that haven't been processed
    global processed_records
    if not processed_records.empty:
        new_records = df[~df["record_id"].isin(processed_records["record_id"])]
    else:
        new_records = df

    if new_records.empty:
        st.warning("No new records to process. All record_ids have already been processed.")
        return processed_records

    # Append new records to the processed records
    processed_records = pd.concat([processed_records, new_records])

    # If there are duplicate record_ids, keep the one with the latest (max) timestamp
    if timestamp_col:
        processed_records["timestamp_sort"] = pd.to_datetime(
            processed_records[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce"
        )
        processed_records = (
            processed_records.sort_values(by="timestamp_sort", ascending=False)
            .drop_duplicates(subset=["record_id"], keep="first")
        )
        processed_records = processed_records.drop(columns=["timestamp_sort"])

    # Save processed records to Firestore
    save_processed_records(processed_records)

    return processed_records

# Streamlit App
st.title("CSV Processor with Firestore Storage")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    df_processed = process_file(uploaded_file)

    if df_processed is not None:
        st.success("File processed successfully!")
        st.dataframe(df_processed)

        # Prepare for download
        buffer = io.BytesIO()
        df_processed.to_csv(buffer, index=False)
        buffer.seek(0)
        
        st.download_button(
            label="Download Processed CSV",
            data=buffer,
            file_name="processed_file.csv",
            mime="text/csv"
        )

# Button to clear processed records in Firestore
if st.button("Clear Processed Records"):
    docs = firestore_db.collection(collection_name).stream()
    for doc in docs:
        doc.reference.delete()
    processed_records = pd.DataFrame()
    st.success("Processed records cleared in Firestore!")

