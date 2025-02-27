import streamlit as st
import pandas as pd
import io
import pytz
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import random
import string

st.title("Firebase Communication Test")

try:
    cred = credentials.Certificate(st.secrets["firebase_service_account"])
    firebase_admin.initialize_app(cred)
except ValueError:
    # Firebase may already be initialized in a Streamlit session.
    pass

db = firestore.client()

# Generate a random string to upload
def generate_random_string(length=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

random_string = generate_random_string()

st.write("Random string generated:", random_string)

if st.button("Upload Random String to Firebase"):
    # Create a new document in the "test_messages" collection
    doc_ref = db.collection("test_messages").document()  # Firestore auto-generates an ID
    doc_ref.set({"message": random_string})
    st.success("Random string uploaded to Firebase!")
    

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

    # If there are duplicate record_ids, keep the one with the latest (max) timestamp
    if timestamp_col:
        df["timestamp_sort"] = pd.to_datetime(df[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce")
        df = df.sort_values(by="timestamp_sort", ascending=False).drop_duplicates(subset=["record_id"], keep="first")
        df = df.drop(columns=["timestamp_sort"])  # Remove the helper column after sorting

    return df

# Streamlit App
st.title("CSV Processor")

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
