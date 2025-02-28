import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import openai  # Make sure you have openai installed and configured

openai.api_key = st.secrets["openai"]["api_key"]

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

# --- Age Mapping Dictionary ---
age_mapping = {
    1: "0 days", 2: "1 day", 3: "2 days", 4: "3 days", 5: "4 days", 6: "5 days",
    7: "6 days", 8: "7 days", 9: "8 days", 10: "9 days", 11: "10 days", 12: "11 days",
    13: "12 days", 14: "13 days", 15: "14 days", 16: "15 days", 17: "16 days",
    18: "17 days", 19: "18 days", 20: "19 days", 21: "20 days", 22: "21 days",
    23: "22 days", 24: "23 days", 25: "24 days", 26: "25 days", 27: "26 days",
    28: "27 days", 29: "28 days", 30: "29 days", 31: "30 days", 32: "1 week",
    33: "2 weeks", 34: "3 weeks", 35: "4 weeks", 36: "5 weeks", 37: "6 weeks",
    38: "7 weeks", 39: "8 weeks", 40: "9 weeks", 41: "10 weeks", 42: "11 weeks",
    43: "12 weeks", 44: "1 month", 45: "2 months", 46: "3 months", 47: "4 months",
    48: "5 months", 49: "6 months", 50: "7 months", 51: "8 months", 52: "9 months",
    53: "10 months", 54: "11 months", 55: "1 year", 56: "2 years", 57: "3 years",
    58: "4 years", 59: "5 years", 60: "6 years", 61: "7 years", 62: "8 years",
    63: "9 years", 64: "10 years", 65: "11 years", 66: "12 years", 67: "13 years",
    68: "14 years", 69: "15 years", 70: "16 years", 71: "17 years", 72: "18 years",
    73: "19 years", 74: "20 years", 75: "21 years", 76: "22 years", 77: "23 years",
    78: "24 years", 79: "25 years", 80: "26 years", 81: "27 years", 82: "28 years",
    83: "29 years", 84: "30 years"
}

# --- Analysis Functions Using OpenAI API ---
def analyze_notes_2_v1(row):
    # Analyze history of present illness
    statement = row['historyofpresentillness_v1']
    age = row['agex_v1']  # Already mapped descriptive age from age_v1
    primary_diagnosis = row['mostlikelydiagnosis_v1']
    
    prompt = (
        f"Assume you are an experienced medical educator evaluating 3rd-year medical students' clinical documentation. "
        f"Review the following history of present illness for a pediatric patient (age: {age}) with the primary diagnosis of {primary_diagnosis}. "
        f"Identify the top 3 essential pieces of information missing or inadequately addressed in the documentation. "
        f"Provide brief constructive feedback on what should be included for a more complete assessment. "
        f"The statement to review is:\n\"{statement}\"\n"
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_4_v1(row):
    # Analyze additional history and physical exam details together
    statement1 = row['additional_hx_v1']
    statement2 = row['vital_signs_and_growth_v1']
    statement3 = row['physicalexam_v1']
    ros = row['reviewofsystems_v1']
    hpi = row['historyofpresentillness_v1']
    age = row['agex_v1']
    primary_diagnosis = row['mostlikelydiagnosis_v1']
    
    prompt = (
        f"Assume you are an experienced medical educator and a harsh grader of medical documentation. "
        f"Review the additional history, HPI, review of systems, and physical exam findings for a pediatric patient (age: {age}) "
        f"with the primary diagnosis of {primary_diagnosis}. Based on this, identify the top 3 essential pieces of information missing "
        f"or inadequately addressed in the documentation. Do not ask for the HPI, primary diagnosis, or physical exam again. "
        f"The statement to review is:\n\"{statement1}\"\n"
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_9_v1(row):
    # Analyze physical examination details
    statement1 = row['vital_signs_and_growth_v1']
    statement2 = row['physicalexam_v1']
    hpi = row['historyofpresentillness_v1']
    age = row['agex_v1']
    primary_diagnosis = row['mostlikelydiagnosis_v1']
    
    prompt = (
        f"Assume you are an experienced medical educator evaluating 3rd-year medical students' clinical documentation. "
        f"Review the physical examination of a pediatric patient (age: {age}) with the primary diagnosis of {primary_diagnosis} "
        f"and the history of present illness (HPI): {hpi}. Identify the top 3 essential pieces of information missing or inadequately addressed in the physical exam. "
        f"The statement to review is:\n\"{statement1} {statement2}\"\n"
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_12_v1(row):
    # Analyze diagnostic justifications and overall documentation
    statement1 = row['additional_hx_v1']
    statement2 = row['vital_signs_and_growth_v1']
    statement3 = row['physicalexam_v1']
    ros = row['reviewofsystems_v1']
    hpi = row['historyofpresentillness_v1']
    age = row['agex_v1']
    dxs = row['dxs_v1']
    primary_diagnosis = row['mostlikelydiagnosis_v1']
    primary_diagnosis_justification = row['mostlikelydiagnosisj_v1']
    secondary_diagnosis = row['seclikelydiagnosis_v1']
    secondary_diagnosis_justification = row['seclikelydiagnosisj_v1']
    third_diagnosis = row['thirlikelydiagnosis_v1']
    third_diagnosis_justification = row['thirlikelydiagnosisj_v1']

    prompt = (
        f"Assume you are an experienced medical educator and a harsh grader of medical documentation. "
        f"Review the following information about a pediatric patient (age: {age}):\n\n"
        f"1. HPI: {hpi}\n"
        f"2. ROS: {ros}\n"
        f"3. Physical Exam: {statement2} {statement3}\n"
        f"4. Additional History: {statement1}\n"
        f"5. Diagnostic Studies: {dxs}\n"
        f"6. Primary Diagnosis: {primary_diagnosis} (Justification: {primary_diagnosis_justification})\n"
        f"7. Secondary Diagnosis: {secondary_diagnosis} (Justification: {secondary_diagnosis_justification})\n"
        f"8. Tertiary Diagnosis: {third_diagnosis} (Justification: {third_diagnosis_justification})\n\n"
        f"Based on this information, please evaluate whether:\n"
        f"- The primary diagnosis has at least three supporting findings from the HPI, ROS, physical exam, or additional history.\n"
        f"- The justifications for all diagnoses are well-written and logically support the diagnoses.\n\n"
        f"Please list any findings that do not support the diagnoses and provide brief explanations."
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_15_v1(row):
    # Analyze grammatical/spelling quality of the documentation
    statement1 = row['additional_hx_v1']
    statement2 = row['vital_signs_and_growth_v1']
    statement3 = row['physicalexam_v1']
    ros = row['reviewofsystems_v1']
    hpi = row['historyofpresentillness_v1']
    age = row['agex_v1']
    dxs = row['dxs_v1']
    primary_diagnosis = row['mostlikelydiagnosis_v1']
    primary_diagnosis_justification = row['mostlikelydiagnosisj_v1']
    secondary_diagnosis = row['seclikelydiagnosis_v1']
    secondary_diagnosis_justification = row['seclikelydiagnosisj_v1']
    third_diagnosis = row['thirlikelydiagnosis_v1']
    third_diagnosis_justification = row['thirlikelydiagnosisj_v1']

    prompt = (
        f"Assume you are an experienced medical educator and a harsh grader of medical documentation. "
        f"Review the following information for a pediatric patient (age: {age}):\n\n"
        f"1. HPI: {hpi}\n"
        f"2. ROS: {ros}\n"
        f"3. Physical Exam: {statement2} {statement3}\n"
        f"4. Additional History: {statement1}\n"
        f"5. Diagnostic Studies: {dxs}\n"
        f"6. Primary Diagnosis: {primary_diagnosis} (Justification: {primary_diagnosis_justification})\n"
        f"7. Secondary Diagnosis: {secondary_diagnosis} (Justification: {secondary_diagnosis_justification})\n"
        f"8. Tertiary Diagnosis: {third_diagnosis} (Justification: {third_diagnosis_justification})\n\n"
        f"Now, check the documentation for grammatical errors, spelling mistakes, and clarity. "
        f"Show me any sentences that need correction along with your suggested revisions."
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

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
    
    # Identify the email column (case-insensitive)
    email_col = next((col for col in df.columns if "email" in col.lower()), None)
    if email_col is None:
        st.error("No email column found in the uploaded file.")
        return None
    df["record_id"] = df[email_col].astype(str)
    
    # Filter out rows already processed in Firestore (version-specific)
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
    
    # Remove duplicate record_ids based on timestamp if applicable
    if timestamp_col:
        df["timestamp_sort"] = pd.to_datetime(df[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce")
        df = df.sort_values(by="timestamp_sort", ascending=False).drop_duplicates(subset=["record_id"], keep="first")
        df = df.drop(columns=["timestamp_sort"])
    
    # Additional processing for version-specific columns (here only v1 is implemented)
    if version == "v1":
        # Process HPI, additional history, vital signs/growth and age mapping
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
        # Map age values
        if "age_v1" in df.columns:
            df["age_v1"] = pd.to_numeric(df["age_v1"], errors="coerce")
            df["agex_v1"] = df["age_v1"].map(age_mapping)
    
    # (For v2, you would replicate similar logic with _v2 columns.)
    
    # Now apply the analysis functions (for v1) on each row.
    # Be aware: these API calls may be slow and consume tokens.
    st.info("Running analysis on new records... (this may take a while)")
    df["notes_2_v1"] = df.apply(analyze_notes_2_v1, axis=1)
    df["notes_4_v1"] = df.apply(analyze_notes_4_v1, axis=1)
    df["notes_9_v1"] = df.apply(analyze_notes_9_v1, axis=1)
    df["notes_12_v1"] = df.apply(analyze_notes_12_v1, axis=1)
    df["notes_15_v1"] = df.apply(analyze_notes_15_v1, axis=1)
    
    # Mark each new record as processed in Firestore (version-specific)
    for record_id in df["record_id"]:
        mark_record_as_processed_version(record_id, version)
    
    return df, version

# --- Streamlit App UI ---
st.title("CSV Processor with Analysis & Version-Specific Firestore Check")

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


