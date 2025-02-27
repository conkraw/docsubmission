import streamlit as st
import pandas as pd
import io

# Load the record_id mapping from Streamlit secrets
def get_record_id_mapping():
    return st.secrets["record_id_mapping"]  # Ensure this is a dictionary in .streamlit/secrets.toml

# Function to process the uploaded file
def process_file(uploaded_file, record_id_mapping):
    df = pd.read_csv(uploaded_file)

    # Ensure 'email' column exists
    if 'email' not in df.columns:
        st.error("No 'email' column found in the uploaded file.")
        return None

    # Drop existing record_id column if present
    if 'record_id' in df.columns:
        df = df.drop(columns=['record_id'])

    # Map emails to new record_id
    df['record_id'] = df['email'].map(record_id_mapping)

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
