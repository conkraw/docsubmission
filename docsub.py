import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase with credentials from Streamlit secrets
cred = credentials.Certificate(st.secrets["firebase_service_account"])
firebase_admin.initialize_app(cred)
db = firestore.client()

# Functions to check and update processed record_ids in Firestore
def is_record_processed(record_id):
    doc_ref = db.collection("processed_records").document(record_id)
    return doc_ref.get().exists

def mark_record_as_processed(record_id):
    doc_ref = db.collection("processed_records").document(record_id)
    doc_ref.set({"processed": True})

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

    # Filter out record_ids that have been processed already
    df = df[~df["record_id"].apply(is_record_processed)]
    
    if df.empty:
        st.info("All record_ids in this file have already been processed.")
        return None

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

    # If there are duplicate record_ids, keep the one with the latest (max) timestamp
    if timestamp_col:
        df["timestamp_sort"] = pd.to_datetime(df[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce")
        df = df.sort_values(by="timestamp_sort", ascending=False).drop_duplicates(subset=["record_id"], keep="first")
        df = df.drop(columns=["timestamp_sort"])  # Remove the helper column after sorting

    # After processing, mark each new record_id as processed in Firebase
    for record_id in df["record_id"]:
        mark_record_as_processed(record_id)

    return df

# Streamlit App
st.title("CSV Processor with Firebase Record Check")

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
