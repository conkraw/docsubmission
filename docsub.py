import streamlit as st
import pandas as pd
import io
import pytz

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

    for original_col, new_col in timestamp_mapping.items():
        if original_col in df.columns:
            # Rename the column
            df = df.rename(columns={original_col: new_col})

            # Convert to datetime
            df[new_col] = pd.to_datetime(df[new_col], errors="coerce")

            # Convert from UTC to Eastern Time
            df[new_col] = df[new_col].dt.tz_localize("UTC").dt.tz_convert("US/Eastern")

            # Format the datetime
            df[new_col] = df[new_col].dt.strftime("%m-%d-%Y %H:%M")

    # Drop the original email column before downloading
    df = df.drop(columns=[email_column_name])

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
