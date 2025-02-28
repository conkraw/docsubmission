import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore

# --- Firebase Initialization ---
firebase_creds = st.secrets["firebase_service_account"].to_dict()
if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)
db = firestore.client()

# --- Firestore Helper Functions (Version-Specific) ---
def is_record_processed_version(record_id, version):
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    return doc_ref.get().exists

def mark_record_as_processed_version(record_id, version):
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    doc_ref.set({"processed": True})

# --- Determine File Version ---
def determine_version(df):
    if any(col.endswith("_v2") for col in df.columns):
        return "v2"
    elif any(col.endswith("_v1") for col in df.columns):
        return "v1"
    else:
        return None

# --- File Processing Function ---
def process_file(uploaded_file):
    df = pd.read_csv(uploaded_file)
    
    # Determine file version
    version = determine_version(df)
    if version is None:
        st.error("No version indicator (_v1 or _v2) found in columns.")
        return None
    
    # Drop existing record_id column if present
    if "record_id" in df.columns:
        df = df.drop(columns=["record_id"])
    
    # Identify an email column (case-insensitive) to use as the unique identifier
    email_col = next((col for col in df.columns if "email" in col.lower()), None)
    if email_col is None:
        st.error("No email column found in the uploaded file.")
        return None
    df["record_id"] = df[email_col].astype(str)
    
    # Filter out rows whose record_id has already been processed (version-specific)
    df = df[~df["record_id"].apply(lambda rid: is_record_processed_version(rid, version))]
    if df.empty:
        st.info("All record_ids in this file have already been processed.")
        return None
    
    # Move record_id to the front
    df = df[["record_id"] + [col for col in df.columns if col != "record_id"]]
    
    # Process timestamp columns (if present)
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
    
    # Drop the original email column
    df = df.drop(columns=[email_col])
    
    # If timestamp was processed, remove duplicates based on record_id (keeping latest timestamp)
    if timestamp_col:
        df["timestamp_sort"] = pd.to_datetime(df[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce")
        df = df.sort_values(by="timestamp_sort", ascending=False).drop_duplicates(subset=["record_id"], keep="first")
        df = df.drop(columns=["timestamp_sort"])
    
    # Additional processing based on version
    if version == "v1":
        # Calculate word count for history of present illness and create additional columns
        df["historyofpresentillness_v1"] = df["historyofpresentillness_v1"].astype(str)
        df["hpiwords_v1"] = df["historyofpresentillness_v1"].apply(lambda x: len(x.split()))
        df["additional_hx_v1"] = (
            'Past Medical History: ' + df["pmhx_v1"].fillna('') + '\n' +
            'Past Surgical History: ' + df["pshx_v1"].fillna('') + '\n' +
            'Family History: ' + df["famhx_v1"].fillna('') + '\n' +
            'Dietary History: ' + df["diet_v1"].fillna('') + '\n' +
            'Birth History: ' + df["birthhx_v1"].fillna('') + '\n' +
            'Developmental History: ' + df["dev_v1"].fillna('') + '\n' +
            'Social History: ' + df["soc_hx_features_v1"].fillna('') + '\n' +
            'Medications: ' + df["med_v1"].fillna('') + '\n' +
            'Allergies: ' + df["all_v1"].fillna('') + '\n' + 
            'Immunizations: ' + df["imm_v1"].fillna('') + '\n'
        )
        df["vital_signs_and_growth_v1"] = (
            'Temperature: ' + df["temp_v1"].astype(str) + '\n' +
            'Heart Rate: ' + df["hr_v1"].astype(str) + '\n' +
            'Respiratory Rate: ' + df["rr_v1"].astype(str) + '\n' +
            'Pulse Oximetry: ' + df["pulseox_v1"].astype(str) + '\n' +
            'Systolic Blood Pressure: ' + df["sbp_v1"].astype(str) + '\n' +
            'Diastolic Blood Pressure: ' + df["dbp_v1"].astype(str) + '\n' +
            'Weight: ' + df["weight_v1"].astype(str) + ' (' + df["weighttile_v1"].astype(str) + ')\n' +
            'Height: ' + df["height_v1"].astype(str) + ' (' + df["heighttile_v1"].astype(str) + ')\n' +
            'BMI: ' + df["bmi_v1"].astype(str) + ' (' + df["bmitile_v1"].astype(str) + ')'
        )
    elif version == "v2":
        df["historyofpresentillness_v2"] = df["historyofpresentillness_v2"].astype(str)
        df["hpiwords_v2"] = df["historyofpresentillness_v2"].apply(lambda x: len(x.split()))
        df["additional_hx_v2"] = (
            'Past Medical History: ' + df["pmhx_v2"].fillna('') + '\n' +
            'Past Surgical History: ' + df["pshx_v2"].fillna('') + '\n' +
            'Family History: ' + df["famhx_v2"].fillna('') + '\n' +
            'Dietary History: ' + df["diet_v2"].fillna('') + '\n' +
            'Birth History: ' + df["birthhx_v2"].fillna('') + '\n' +
            'Developmental History: ' + df["dev_v2"].fillna('') + '\n' +
            'Social History: ' + df["soc_hx_features_v2"].fillna('') + '\n' +
            'Medications: ' + df["med_v2"].fillna('') + '\n' +
            'Allergies: ' + df["all_v2"].fillna('') + '\n' +
            'Immunizations: ' + df["imm_v2"].fillna('') + '\n'
        )
        df["vital_signs_and_growth_v2"] = (
            'Temperature: ' + df["temp_v2"].astype(str) + '\n' +
            'Heart Rate: ' + df["hr_v2"].astype(str) + '\n' +
            'Respiratory Rate: ' + df["rr_v2"].astype(str) + '\n' +
            'Pulse Oximetry: ' + df["pulseox_v2"].astype(str) + '\n' +
            'Systolic Blood Pressure: ' + df["sbp_v2"].astype(str) + '\n' +
            'Diastolic Blood Pressure: ' + df["dbp_v2"].astype(str) + '\n' +
            'Weight: ' + df["weight_v2"].astype(str) + ' (' + df["weighttile_v2"].astype(str) + ')\n' +
            'Height: ' + df["height_v2"].astype(str) + ' (' + df["heighttile_v2"].astype(str) + ')\n' +
            'BMI: ' + df["bmi_v2"].astype(str) + ' (' + df["bmitile_v2"].astype(str) + ')'
        )
    
    # Mark each new record as processed in the version-specific Firestore collection.
    for record_id in df["record_id"]:
        mark_record_as_processed_version(record_id, version)
    
    return df, version

# --- Streamlit App UI ---
st.title("CSV Processor with Version-Specific Firestore Check")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
if uploaded_file:
    result = process_file(uploaded_file)
    if result is not None:
        df_processed, version = result
        st.success("File processed successfully!")
        st.dataframe(df_processed)
        output_filename = f"processed_file_{version}.csv"
        buffer = io.BytesIO()
        df_processed.to_csv(buffer, index=False)
        buffer.seek(0)
        st.download_button(
            label="Download Processed CSV",
            data=buffer,
            file_name=output_filename,
            mime="text/csv"
        )

