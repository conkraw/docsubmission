import streamlit as st
import pandas as pd
import io

# Function to extract the record_id mapping from secrets
def get_record_id_mapping():
    try:
        dataset = st.secrets["dataset"]["data"]  # Accessing the list of dictionaries
        mapping = {}

        for entry in dataset:
            if "email" in entry and "record_id" in entry:
                mapping[entry["email"].strip().lower()] = entry["record_id"]
            if "email_2" in entry and "record_id" in entry:
                mapping[entry["email_2"].strip().lower()] = entry["record_id"]

        return mapping
    except KeyError:
        st.error("Error: Secrets structure is incorrect.")
        return {}

# Function to process the uploaded file
def process_file(uploaded_file, record_id_mapping):
    df = pd.read_csv(uploaded_file)

    # Identify the column containing "email" dynamically
    email_column_name = next((col for col in df.columns if "email" in col.lower()), None)
    
    if email_column_name is None:
        st.error("No email column found in the uploaded file.")
        return None

    # Drop existing record_id column if present
    if 'record_id' in df.columns:
        df = df.drop(columns=['record_id'])

    # Map emails to new record_id (handling case-sensitivity)
    df['record_id'] = df[email_column_name].str.strip().str.lower().map(record_id_mapping)

    # Check if there are unmapped emails
    unmatched_emails = df[df['record_id'].isna()][email_column_name].unique()
    if len(unmatched_emails) > 0:
        st.warning(f"Some emails were not found in the record_id mapping: {unmatched_emails}")

    # Move record_id to the front
    cols = ['record_id'] + [col for col in df.columns if col != 'record_id']
    df = df[cols]

    return df

# Streamlit App
st.title("CSV Processor")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    record_id_mapping = get_record_id_mapping()

    df_processed = process_file(uploaded_file, record_id_mapping)

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

