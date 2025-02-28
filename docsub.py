import streamlit as st
import pandas as pd
import io
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import openai

#############################
# 1) OPENAI INITIALIZATION #
#############################
openai.api_key = st.secrets["openai"]["api_key"]

#############################
# 2) FIREBASE INITIALIZATION
#############################
firebase_creds = st.secrets["firebase_service_account"].to_dict()
if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)

db = firestore.client()

#############################
# 3) FIRESTORE HELPERS
#############################
def is_record_processed_version(record_id, version):
    """Check if record_id is already in the version-specific Firestore collection."""
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    return doc_ref.get().exists

def mark_record_as_processed_version(record_id, version):
    """Mark record_id as processed in the version-specific Firestore collection."""
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    doc_ref.set({"processed": True})

#############################
# 4) VERSION DETECTION
#############################
def determine_version(df):
    """
    Returns "v2" if any column ends with _v2,
    returns "v1" if any column ends with _v1,
    otherwise returns None.
    """
    if any(col.endswith("_v2") for col in df.columns):
        return "v2"
    elif any(col.endswith("_v1") for col in df.columns):
        return "v1"
    else:
        return None

#############################
# 5) AGE MAPPING
#############################
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

#############################
# 6) ANALYSIS FUNCTIONS (V1)
#############################
def analyze_notes_2_v1(row):
    statement = row['historyofpresentillness_v1']
    age = row['agex_v1']
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
        f"with the primary diagnosis of {primary_diagnosis}. Identify the top 3 essential pieces of information missing "
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

#############################
# 7) ANALYSIS FUNCTIONS (V2)
#############################
# For _v2, replicate the logic but reference _v2 columns.
# We'll show an example for notes_2_v2. The rest are analogous.
# If you prefer a dynamic approach, you can do that, but here's a direct approach:

def analyze_notes_2_v2(row):
    statement = row['historyofpresentillness_v2']
    age = row['agex_v2']
    primary_diagnosis = row['mostlikelydiagnosis_v2']
    
    prompt = (
        f"[v2 LOGIC] Assume you are an experienced medical educator evaluating 3rd-year medical students' clinical documentation. "
        f"Review the following history of present illness for a pediatric patient (age: {age}) with the primary diagnosis of {primary_diagnosis}. "
        f"Identify the top 3 essential pieces of information missing or inadequately addressed in the documentation. "
        f"Provide brief constructive feedback. "
        f"The statement to review is:\n\"{statement}\"\n"
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

# Similarly define analyze_notes_4_v2, notes_9_v2, notes_12_v2, notes_15_v2...
# We'll skip full detail for brevity, but you can replicate the v1 structure
# referencing _v2 columns.

#############################
# 8) PROCESS FILE
#############################
def process_file(uploaded_file):
    df = pd.read_csv(uploaded_file)

    # 1) Determine version
    version = determine_version(df)
    if version is None:
        st.error("No version indicator (_v1 or _v2) found in columns.")
        return None

    # 2) Drop existing record_id if any
    if "record_id" in df.columns:
        df.drop(columns=["record_id"], inplace=True)

    # 3) Identify email column
    email_col = next((col for col in df.columns if "email" in col.lower()), None)
    if not email_col:
        st.error("No email column found.")
        return None

    df["record_id"] = df[email_col].astype(str)

    # 4) Filter out processed records
    df = df[~df["record_id"].apply(lambda rid: is_record_processed_version(rid, version))]
    if df.empty:
        st.info("All record_ids have already been processed.")
        return None

    # Move record_id to front
    df = df[["record_id"] + [c for c in df.columns if c != "record_id"]]

    # 5) Timestamps
    timestamp_mapping = {
        "documentation_submission_1_timestamp": "peddoclate1",
        "documentation_submission_2_timestamp": "peddoclate2"
    }
    timestamp_col = None
    for original_col, new_col in timestamp_mapping.items():
        if original_col in df.columns:
            df.rename(columns={original_col: new_col}, inplace=True)
            timestamp_col = new_col
            df[new_col] = pd.to_datetime(df[new_col], errors="coerce")
            df[new_col] = df[new_col].dt.tz_localize("UTC").dt.tz_convert("US/Eastern")
            df[new_col] = df[new_col].dt.strftime("%m-%d-%Y %H:%M")

    df.drop(columns=[email_col], inplace=True)

    if timestamp_col:
        df["timestamp_sort"] = pd.to_datetime(df[timestamp_col], format="%m-%d-%Y %H:%M", errors="coerce")
        df = df.sort_values(by="timestamp_sort", ascending=False).drop_duplicates(subset=["record_id"], keep="first")
        df.drop(columns=["timestamp_sort"], inplace=True)

    # 6) If version == v1, do v1 logic
    if version == "v1":
        # Convert some columns to str, build additional hx, etc.
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
        # Age mapping
        if "age_v1" in df.columns:
            df["age_v1"] = pd.to_numeric(df["age_v1"], errors="coerce")
            df["agex_v1"] = df["age_v1"].map(age_mapping)

        st.info("Running analysis on new v1 records... (this may take a while)")
        df["notes_2_v1"] = df.apply(analyze_notes_2_v1, axis=1)
        df["notes_4_v1"] = df.apply(analyze_notes_4_v1, axis=1)
        df["notes_9_v1"] = df.apply(analyze_notes_9_v1, axis=1)
        df["notes_12_v1"] = df.apply(analyze_notes_12_v1, axis=1)
        df["notes_15_v1"] = df.apply(analyze_notes_15_v1, axis=1)

    # 7) If version == v2, replicate the logic but referencing _v2 columns
    elif version == "v2":
        # Example: hpiwords_v2, additional_hx_v2, vital_signs_and_growth_v2, etc.
        if "historyofpresentillness_v2" in df.columns:
            df["historyofpresentillness_v2"] = df["historyofpresentillness_v2"].astype(str)
            df["hpiwords_v2"] = df["historyofpresentillness_v2"].apply(lambda x: len(x.split()))

        if "age_v2" in df.columns:
            df["age_v2"] = pd.to_numeric(df["age_v2"], errors="coerce")
            df["agex_v2"] = df["age_v2"].map(age_mapping)

        # Build additional columns similarly to v1
        # ...
        # Then call your v2 analysis functions (like analyze_notes_2_v2, etc.)
        st.info("Running analysis on new v2 records... (this may take a while)")
        df["notes_2_v2"] = df.apply(analyze_notes_2_v2, axis=1)
        # df["notes_4_v2"] = df.apply(analyze_notes_4_v2, axis=1)
        # df["notes_9_v2"] = df.apply(analyze_notes_9_v2, axis=1)
        # df["notes_12_v2"] = df.apply(analyze_notes_12_v2, axis=1)
        # df["notes_15_v2"] = df.apply(analyze_notes_15_v2, axis=1)

    # Mark each new record as processed
    for rid in df["record_id"]:
        mark_record_as_processed_version(rid, version)

    return df, version

#############################
# 9) STREAMLIT UI
#############################
st.title("CSV Processor with Analysis & Version-Specific Firestore Check")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
if uploaded_file:
    result = process_file(uploaded_file)
    if result is not None:
        df_processed, version = result
        st.success("File processed successfully!")
        st.dataframe(df_processed)

        # Drop columns before download
        if version == "v1":
            columns_to_remove = ["additional_hx_v1", "vital_signs_and_growth_v1", "agex_v1"]
        elif version == "v2":
            columns_to_remove = ["additional_hx_v2", "vital_signs_and_growth_v2", "agex_v2"]
        else:
            columns_to_remove = []

        for col in columns_to_remove:
            if col in df_processed.columns:
                df_processed.drop(columns=[col], inplace=True)

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

