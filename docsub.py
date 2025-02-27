import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, db
import json
import os

# Load Firebase credentials from Streamlit secrets
if not firebase_admin._apps:
    firebase_creds = json.loads(st.secrets["firebase"]["private_key"].replace("\\n", "\n"))
    cred = credentials.Certificate({
        "type": "service_account",
        "project_id": st.secrets["firebase"]["project_id"],
        "private_key_id": st.secrets["firebase"]["private_key_id"],
        "private_key": firebase_creds,
        "client_email": st.secrets["firebase"]["client_email"],
        "client_id": st.secrets["firebase"]["client_id"],
        "auth_uri": st.secrets["firebase"]["auth_uri"],
        "token_uri": st.secrets["firebase"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["firebase"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["firebase"]["client_x509_cert_url"],
    })
    firebase_admin.initialize_app(cred, {"databaseURL": st.secrets["firebase"]["database_url"]})

# Firebase reference
firebase_ref = db.reference("processed_records")

# Load existing processed records from Firebase
def load_processed_records():
    data = firebase_ref.get()
    if data:
        return pd.DataFrame.from_dict(data, orient="index")
    return pd.DataFrame()

# Function to save processed records to Firebase
def save_processed_records(df):
    records_dict = df.to_dict(orient="index")
    firebase_ref.set(records_dict)

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

    # Save processed records to Firebase
    save_processed_records(processed_records)

    return processed_records

# Streamlit App
st.title("CSV Processor with Firebase Storage")

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

# Button to clear processed records in Firebase
if st.button("Clear Processed Records"):
    firebase_ref.delete()
    processed_records = pd.DataFrame()
    st.success("Processed records cleared in Firebase!")
