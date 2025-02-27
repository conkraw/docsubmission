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
                mapping[entry["email"]] = entry["record_id"]
            if "email_2" in entry and "record_id" in entry:
                mapping[entry["email_2"]] = entry["record_id"]

        return mapping
    except KeyError:
        st.error("Error: Secrets structure is incorrect.")
        return {}

# Function to process the uploaded file
def process_file(uploaded_file, record_id_mapping):
    df = pd.read_csv(uploaded_file)

    # Ensure there are at least 3 columns
    if df.shape[1] < 3:
        st.error("The uploaded file must have at least 3 columns.")
        return None

    # Drop existing record_id column if present
    if 'record_id' in df.columns:
        df = df.drop(columns=['record_id'])

    # Identify email column (3rd column, index 2)
    email_column_name = df.columns[2]
    
    # Map emails to new record_id
    df['record_id'] = df[email_column_name].map(record_id_mapping)

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

