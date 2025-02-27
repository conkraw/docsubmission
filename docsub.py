import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import json

# Initialize Firebase once using session_state
if "firebase_initialized" not in st.session_state:
    try:
        firebase_key = st.secrets["FIREBASE_KEY"]
        cred = credentials.Certificate(json.loads(firebase_key))
        firebase_admin.initialize_app(cred)
        st.session_state.firebase_initialized = True
    except ValueError as e:
        if "already exists" in str(e):
            st.session_state.firebase_initialized = True  # Firebase already initialized
        else:
            st.error(f"🔥 Failed to initialize Firebase: {str(e)}")
            st.stop()

# Ensure Firestore client is available
if "db" not in st.session_state:
    try:
        st.session_state.db = firestore.client()
    except Exception as e:
        st.error(f"🔥 Failed to connect to Firestore: {str(e)}")
        st.stop()

# Access Firestore client from session state
db = st.session_state.db

# Function to load processed records from Firestore
def load_processed_records():
    collection_name = st.secrets["FIREBASE_COLLECTION_NAME"]  # Get collection name
    docs = db.collection(collection_name).stream()
    records = {doc.id: doc.to_dict() for doc in docs}
    return pd.DataFrame.from_dict(records, orient="index") if records else pd.DataFrame()

# Load processed records at the start
processed_records_df = load_processed_records()

# Function to process the uploaded file
def process_file(uploaded_file):
    df = pd.read_csv(uploaded_file)

    # Drop existing record_id column if present
    if "record_id" in df.columns:
        df = df.drop(columns=["record_id"])

    # Identify email column dynamically
    email_column_name = next((col for col in df.columns if "email" in col.lower()), None)
    
    if email_column_name is None:
        st.error("No email column found in the uploaded file.")
        return None

    # Rename email column to record_id
    df["record_id"] = df[email_column_name].astype(str)

    # Move record_id to the front
    cols = ["record_id"] + [col for col in df.columns if col != "record_id"]
    df = df[cols]

    # Handle timestamp conversion
    timestamp_mapping = {
        "documentation_submission_1_timestamp": "peddoclate1",
        "documentation_submission_2_timestamp": "peddoclate2"
    }

    timestamp_col = None
    for original_col, new_col in timestamp_mapping.items():
        if original_col in df.columns:
            df = df.rename(columns={original_col: new_col})
            timestamp_col = new_col

            df[new_col] = pd.to_datetime(df[new_col], errors="coerce")
            df[new_col] = df[new_col].dt.tz_localize("UTC").dt.tz_convert("US/Eastern")
            df[new_col] = df[new_col].dt.strftime("%m-%d-%Y %H:%M")

    # Drop email column
    df = df.drop(columns=[email_column_name])

    # Identify new records
    global processed_records_df
    if not processed_records_df.empty:
        new_records = df[~df["record_id"].isin(processed_records_df["record_id"])]
    else:
        new_records = df

    if new_records.empty:
        st.warning("No new records to process. All record_ids have already been processed.")
        return processed_records_df

    # Merge with existing records
    processed_records_df = pd.concat([processed_records_df, new_records])

    # Keep the latest timestamp record
    if timestamp_col:
        processed_records_df["timestamp_sort"] = pd.to_datetime(
            processed_records_df[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce"
        )
        processed_records_df = (
            processed_records_df.sort_values(by="timestamp_sort", ascending=False)
            .drop_duplicates(subset=["record_id"], keep="first")
        )
        processed_records_df = processed_records_df.drop(columns=["timestamp_sort"])

    # Upload new records to Firestore
    for _, row in processed_records_df.iterrows():
        db.collection(st.secrets["FIREBASE_COLLECTION_NAME"]).document(row["record_id"]).set(row.to_dict(), merge=True)

    return processed_records_df

# Streamlit App
st.title("CSV Processor with Firestore")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    df_processed = process_file(uploaded_file)

    if df_processed is not None:
        st.success("✅ File processed successfully!")
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
    docs = db.collection(st.secrets["FIREBASE_COLLECTION_NAME"]).stream()
    for doc in docs:
        doc.reference.delete()
    processed_records_df = pd.DataFrame()
    st.success("✅ Processed records cleared in Firestore!")


