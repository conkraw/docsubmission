import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import json

import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import json

st.title("Firebase Initialization Debug")

# Retrieve your Firebase key from st.secrets
firebase_key = st.secrets.get("firebase")
collection_name = st.secrets.get("FIREBASE_COLLECTION_NAME")

st.write("Type of firebase_key:", type(firebase_key))
st.write("Raw firebase_key value:")
st.text(firebase_key)

# If you're using triple quotes, firebase_key will be a string with actual newline characters.
if isinstance(firebase_key, str):
    # Replace literal newline characters with the two-character sequence "\n"
    firebase_key_fixed = firebase_key.replace("\n", "\\n")
    st.write("After replacing newlines:")
    st.text(firebase_key_fixed)
    try:
        firebase_creds = json.loads(firebase_key_fixed)
    except Exception as e:
        st.error(f"Error parsing firebase_key: {e}")
        st.stop()
else:
    firebase_creds = firebase_key  # if it's already a dict

st.write("Parsed firebase_creds keys:", list(firebase_creds.keys()))

# Check that required keys are present
required_keys = ["client_email", "token_uri", "private_key", "project_id"]
missing = [key for key in required_keys if key not in firebase_creds]
if missing:
    st.error(f"Missing required keys in Firebase credentials: {missing}")
    st.stop()

# Initialize Firebase
if "firebase_initialized" not in st.session_state:
    try:
        cred = credentials.Certificate(firebase_creds)
        firebase_admin.initialize_app(cred)
        st.session_state["firebase_initialized"] = True
        st.write("Firebase initialized successfully!")
    except Exception as e:
        st.error(f"Error initializing Firebase: {e}")
        st.stop()

# Create Firestore client
if "db" not in st.session_state:
    try:
        st.session_state["db"] = firestore.client()
        st.write("Firestore client created.")
    except Exception as e:
        st.error(f"Error connecting to Firestore: {e}")
        st.stop()

st.write("Collection name:", collection_name)

db = st.session_state["db"]

st.write("Collection name:", collection_name)

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

