import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import json

# --- Firebase Initialization ---
# Get Firebase credentials and collection name from st.secrets
firebase_key = st.secrets.get("firebase")
collection_name = st.secrets.get("FIREBASE_COLLECTION_NAME")

if firebase_key is None:
    st.error("FIREBASE_KEY is missing in st.secrets!")
    st.stop()
if collection_name is None:
    st.error("FIREBASE_COLLECTION_NAME is missing in st.secrets!")
    st.stop()

# If firebase_key is a dict (from TOML), use it directly; if it's a string, parse it.
if isinstance(firebase_key, dict):
    firebase_creds = firebase_key
elif isinstance(firebase_key, str):
    try:
        # Replace escaped newlines with actual newlines before parsing.
        firebase_creds = json.loads(firebase_key.replace("\\n", "\n"))
    except Exception as e:
        st.error(f"Error parsing FIREBASE_KEY: {e}")
        st.stop()
else:
    st.error("FIREBASE_KEY must be a dict or a JSON-formatted string.")
    st.stop()

# Initialize Firebase only once.
if "firebase_initialized" not in st.session_state:
    try:
        cred = credentials.Certificate(firebase_creds)
        firebase_admin.initialize_app(cred)
        st.session_state["firebase_initialized"] = True
    except ValueError as e:
        if "already exists" in str(e):
            st.session_state["firebase_initialized"] = True
        else:
            st.error(f"Error initializing Firebase: {e}")
            st.stop()

# Get Firestore client.
if "db" not in st.session_state:
    try:
        st.session_state["db"] = firestore.client()
    except Exception as e:
        st.error(f"Error connecting to Firestore: {e}")
        st.stop()

db = st.session_state["db"]

# --- Function to Upload a Record ---
def upload_record(document_id, data):
    try:
        db.collection(collection_name).document(document_id).set(data, merge=True)
    except Exception as e:
        st.error(f"Error uploading record {document_id}: {e}")

# --- Main App Code ---
st.title("CSV Processor with Firebase Integration")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    # Remove any existing 'record_id' column.
    if "record_id" in df.columns:
        df = df.drop(columns=["record_id"])

    # Identify an email column (first column with "email", case-insensitive).
    email_column = next((col for col in df.columns if "email" in col.lower()), None)
    if email_column is None:
        st.error("No email column found in the CSV file.")
    else:
        # Rename the email column to 'record_id'.
        df["record_id"] = df[email_column].astype(str)
        # Move 'record_id' to the front.
        cols = ["record_id"] + [col for col in df.columns if col != "record_id"]
        df = df[cols]

        # Handle timestamp conversion.
        timestamp_mapping = {
            "documentation_submission_1_timestamp": "peddoclate1",
            "documentation_submission_2_timestamp": "peddoclate2"
        }
        for orig, new in timestamp_mapping.items():
            if orig in df.columns:
                df = df.rename(columns={orig: new})
                df[new] = pd.to_datetime(df[new], errors="coerce")
                df[new] = df[new].dt.tz_localize("UTC").dt.tz_convert("US/Eastern")
                df[new] = df[new].dt.strftime("%m-%d-%Y %H:%M")

        # Drop the original email column.
        df = df.drop(columns=[email_column])

        # Upload each record to Firestore using 'record_id' as the document ID.
        for _, row in df.iterrows():
            document_id = row["record_id"]
            data = row.to_dict()
            upload_record(document_id, data)

        st.success("CSV processed and data uploaded to Firebase successfully!")

        # Provide a download button for the processed CSV.
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        st.download_button("Download Processed CSV", buffer, file_name="processed_file.csv", mime="text/csv")

