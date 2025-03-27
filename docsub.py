import streamlit as st
import pandas as pd
import io
import re
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
import openai

########################################
# 1) OPENAI & FIREBASE SETUP
########################################

openai.api_key = st.secrets["openai"]["api_key"]

firebase_creds = st.secrets["firebase_service_account"].to_dict()
if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_creds)
    firebase_admin.initialize_app(cred)

db = firestore.client()

########################################
# 2) FIRESTORE HELPERS
########################################

def is_record_processed_version(record_id, version):
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    return doc_ref.get().exists

def mark_record_as_processed_version(record_id, version):
    collection_name = f"processed_records_{version}"
    doc_ref = db.collection(collection_name).document(record_id)
    doc_ref.set({"processed": True})

########################################
# 3) DETERMINE FILE VERSION
########################################

def determine_version(df):
    """
    Returns 'v2' if any column ends with '_v2',
    returns 'v1' if any column ends with '_v1',
    otherwise returns None.
    """
    if any(col.endswith("_v2") for col in df.columns):
        return "v2"
    elif any(col.endswith("_v1") for col in df.columns):
        return "v1"
    else:
        return None

########################################
# 4) LINE BREAK INSERTION
########################################

def insert_line_breaks(text):
    """
    Insert a newline after a period (.) followed by two or more spaces.
    e.g., "Normal exam.  Abdomen: Soft..." -> "Normal exam.\nAbdomen: Soft..."
    """
    if not isinstance(text, str):
        return text
    return re.sub(r'\.\s{2,}', '.\n', text)

########################################
# 5) AGE MAPPING
########################################

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

########################################
# 6) BUILDING ADDITIONAL COLUMNS (DYNAMIC)
########################################

def build_additional_columns(df, version):
    """
    Dynamically build 'additional_hx_{version}' and 'vital_signs_and_growth_{version}'
    if the relevant source columns exist, referencing pmhx_{version}, pshx_{version}, etc.
    """
    # 1) Build 'additional_hx_{version}' if pmhx_{version}, pshx_{version}, etc. exist
    pmhx_col  = f"pmhx_{version}"
    pshx_col  = f"pshx_{version}"
    famhx_col = f"famhx_{version}"
    diet_col  = f"diet_{version}"
    birth_col = f"birthhx_{version}"
    dev_col   = f"dev_{version}"
    soc_col   = f"soc_hx_features_{version}"
    med_col   = f"med_{version}"
    all_col   = f"all_{version}"
    imm_col   = f"imm_{version}"

    add_col_name = f"additional_hx_{version}"
    # Only build if the columns exist
    if pmhx_col in df.columns:
        df[add_col_name] = (
            'Past Medical History: ' + df[pmhx_col].fillna('') + '\n' +
            'Past Surgical History: ' + df.get(pshx_col, '').fillna('') + '\n' +
            'Family History: ' + df.get(famhx_col, '').fillna('') + '\n' +
            'Dietary History: ' + df.get(diet_col, '').fillna('') + '\n' +
            'Birth History: ' + df.get(birth_col, '').fillna('') + '\n' +
            'Developmental History: ' + df.get(dev_col, '').fillna('') + '\n' +
            'Social History: ' + df.get(soc_col, '').fillna('') + '\n' +
            'Medications: ' + df.get(med_col, '').fillna('') + '\n' +
            'Allergies: ' + df.get(all_col, '').fillna('') + '\n' +
            'Immunizations: ' + df.get(imm_col, '').fillna('') + '\n'
        )

    # 2) Build 'vital_signs_and_growth_{version}' if temp_{version}, hr_{version}, etc. exist
    temp_col   = f"temp_{version}"
    hr_col     = f"hr_{version}"
    rr_col     = f"rr_{version}"
    pulseox_col= f"pulseox_{version}"
    sbp_col    = f"sbp_{version}"
    dbp_col    = f"dbp_{version}"
    weight_col = f"weight_{version}"
    weighttile_col = f"weighttile_{version}"
    height_col = f"height_{version}"
    heighttile_col = f"heighttile_{version}"
    bmi_col    = f"bmi_{version}"
    bmitile_col= f"bmitile_{version}"

    vit_col_name = f"vital_signs_and_growth_{version}"
    if temp_col in df.columns:
        df[vit_col_name] = (
            'Temperature: ' + df.get(temp_col, '').astype(str) + '\n' +
            'Heart Rate: ' + df.get(hr_col, '').astype(str) + '\n' +
            'Respiratory Rate: ' + df.get(rr_col, '').astype(str) + '\n' +
            'Pulse Oximetry: ' + df.get(pulseox_col, '').astype(str) + '\n' +
            'Systolic Blood Pressure: ' + df.get(sbp_col, '').astype(str) + '\n' +
            'Diastolic Blood Pressure: ' + df.get(dbp_col, '').astype(str) + '\n' +
            'Weight: ' + df.get(weight_col, '').astype(str) + ' (' + df.get(weighttile_col, '').fillna('') + ')\n' +
            'Height: ' + df.get(height_col, '').astype(str) + ' (' + df.get(heighttile_col, '').fillna('') + ')\n' +
            'BMI: ' + df.get(bmi_col, '').astype(str) + ' (' + df.get(bmitile_col, '').fillna('') + ')'
        )

########################################
# 7) AI ANALYSIS (DYNAMIC)
########################################

def analyze_notes_2(row, version):
    """
    references columns: historyofpresentillness_{version}, agex_{version}, mostlikelydiagnosis_{version}
    """
    hpi_col  = f"historyofpresentillness_{version}"
    agex_col = f"agex_{version}"
    dx_col   = f"mostlikelydiagnosis_{version}"

    statement = row.get(hpi_col, "")
    age = row.get(agex_col, "")
    dx  = row.get(dx_col, "")

    prompt = (
        f"Assume you are an experienced medical educator evaluating 3rd-year medical students' clinical documentation. "
        f"Please review the following history of present illness for a pediatric patient (age: {age}) "
        f"with the primary diagnosis of {dx}. "
        f"Identify the top 3 essential pieces of information missing or inadequately addressed in the documentation. "
        f"Provide brief constructive feedback on what should be included. "
        f"The statement to review is:\n\"{statement}\"\n"
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_4(row, version):
    add_col   = f"additional_hx_{version}"
    vit_col   = f"vital_signs_and_growth_{version}"
    pe_col    = f"physicalexam_{version}"
    ros_col   = f"reviewofsystems_{version}"
    hpi_col   = f"historyofpresentillness_{version}"
    agex_col  = f"agex_{version}"
    dx_col    = f"mostlikelydiagnosis_{version}"

    statement1 = row.get(add_col, "")
    statement2 = row.get(vit_col, "")
    statement3 = row.get(pe_col, "")
    ros        = row.get(ros_col, "")
    hpi        = row.get(hpi_col, "")
    age        = row.get(agex_col, "")
    dx         = row.get(dx_col, "")

    prompt = (
        f"Assume you are an experienced medical educator and a harsh grader of medical documentation. "
        f"Please review the additional history of a pediatric patient (age: {age}), the HPI: {hpi}, "
        f"ROS: {ros}, and physical exam (Physical Exam: {statement2} {statement3}), "
        f"along with the primary diagnosis ({dx}). Identify the top 3 essential pieces of information missing "
        f"or inadequately addressed. The statement to review is:\n\"{statement1}\"\n\n"
        f"Do not ask for the HPI, primary diagnosis, or physical exam again as this information is already provided."
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_9(row, version):
    vit_col  = f"vital_signs_and_growth_{version}"
    pe_col   = f"physicalexam_{version}"
    hpi_col  = f"historyofpresentillness_{version}"
    agex_col = f"agex_{version}"
    dx_col   = f"mostlikelydiagnosis_{version}"

    statement1 = row.get(vit_col, "")
    statement2 = row.get(pe_col, "")
    hpi        = row.get(hpi_col, "")
    age        = row.get(agex_col, "")
    dx         = row.get(dx_col, "")

    prompt = (
        f"Assume you are an experienced medical educator evaluating 3rd-year medical students' clinical documentation. "
        f"Please review the physical examination of a pediatric patient (age: {age}) with the primary diagnosis of {dx} "
        f"and the history of present illness (HPI): {hpi}. Identify the top 3 essential pieces of information missing or "
        f"inadequately addressed in the physical exam. "
        f"The statement to review is:\n\"{statement1} {statement2}\"\n"
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_12(row, version):
    add_col   = f"additional_hx_{version}"
    vit_col   = f"vital_signs_and_growth_{version}"
    pe_col    = f"physicalexam_{version}"
    ros_col   = f"reviewofsystems_{version}"
    hpi_col   = f"historyofpresentillness_{version}"
    agex_col  = f"agex_{version}"
    dxs_col   = f"dxs_{version}"
    dx_col    = f"mostlikelydiagnosis_{version}"
    dxj_col   = f"mostlikelydiagnosisj_{version}"
    secdx_col = f"seclikelydiagnosis_{version}"
    secjx_col = f"seclikelydiagnosisj_{version}"
    thrdx_col = f"thirlikelydiagnosis_{version}"
    thrjx_col = f"thirlikelydiagnosisj_{version}"

    statement1 = row.get(add_col, "")
    statement2 = row.get(vit_col, "")
    statement3 = row.get(pe_col, "")
    ros        = row.get(ros_col, "")
    hpi        = row.get(hpi_col, "")
    age        = row.get(agex_col, "")
    dxs        = row.get(dxs_col, "")
    dx         = row.get(dx_col, "")
    dxj        = row.get(dxj_col, "")
    secdx      = row.get(secdx_col, "")
    secjx      = row.get(secjx_col, "")
    thrdx      = row.get(thrdx_col, "")
    thrjx      = row.get(thrjx_col, "")

    prompt = (
        f"Assume you are an experienced medical educator and a harsh grader of medical documentation. "
        f"Please review the following information about a pediatric patient (age: {age}):\n\n"
        f"1. HPI: {hpi}\n"
        f"2. ROS: {ros}\n"
        f"3. Physical Exam: {statement2} {statement3}\n"
        f"4. Additional History: {statement1}\n"
        f"5. Diagnostic Studies: {dxs}\n"
        f"6. Primary Diagnosis: {dx} (Justification: {dxj})\n"
        f"7. Secondary Diagnosis: {secdx} (Justification: {secjx})\n"
        f"8. Tertiary Diagnosis: {thrdx} (Justification: {thrjx})\n\n"
        f"Does the primary diagnosis have at least three supporting findings?\n"
        f"Are the secondary and tertiary diagnoses appropriate?\n"
        f"Do not ask for HPI, ROS, physical exam, or diagnoses again, as all details are provided."
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

def analyze_notes_15(row, version):
    add_col   = f"additional_hx_{version}"
    vit_col   = f"vital_signs_and_growth_{version}"
    pe_col    = f"physicalexam_{version}"
    ros_col   = f"reviewofsystems_{version}"
    hpi_col   = f"historyofpresentillness_{version}"
    agex_col  = f"agex_{version}"
    dxs_col   = f"dxs_{version}"
    dx_col    = f"mostlikelydiagnosis_{version}"
    dxj_col   = f"mostlikelydiagnosisj_{version}"
    secdx_col = f"seclikelydiagnosis_{version}"
    secjx_col = f"seclikelydiagnosisj_{version}"
    thrdx_col = f"thirlikelydiagnosis_{version}"
    thrjx_col = f"thirlikelydiagnosisj_{version}"

    statement1 = row.get(add_col, "")
    statement2 = row.get(vit_col, "")
    statement3 = row.get(pe_col, "")
    ros        = row.get(ros_col, "")
    hpi        = row.get(hpi_col, "")
    age        = row.get(agex_col, "")
    dxs        = row.get(dxs_col, "")
    dx         = row.get(dx_col, "")
    dxj        = row.get(dxj_col, "")
    secdx      = row.get(secdx_col, "")
    secjx      = row.get(secjx_col, "")
    thrdx      = row.get(thrdx_col, "")
    thrjx      = row.get(thrjx_col, "")

    prompt = (
        f"Assume you are an experienced medical educator and a harsh grader of medical documentation. "
        f"Please review the following info for a pediatric patient (age: {age}):\n\n"
        f"1. HPI: {hpi}\n"
        f"2. ROS: {ros}\n"
        f"3. Physical Exam: {statement2} {statement3}\n"
        f"4. Additional History: {statement1}\n"
        f"5. Diagnostic Studies: {dxs}\n"
        f"6. Primary Diagnosis: {dx} (Justification: {dxj})\n"
        f"7. Secondary Diagnosis: {secdx} (Justification: {secjx})\n"
        f"8. Tertiary Diagnosis: {thrdx} (Justification: {thrjx})\n\n"
        f"Check for grammatical/spelling errors and clarity in the HPI and diagnosis justifications. "
        f"Show me any problematic sentences and suggest revisions."
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500
    )
    return response['choices'][0]['message']['content']

########################################
# 8) PROCESS FILE
########################################

def process_file(uploaded_file):
    """
    Reads CSV as strings, determines version, filters out processed records,
    inserts line breaks, builds dynamic columns, runs dynamic AI analysis,
    marks records processed, returns (df, version).
    """
    # 1) Read all as strings to preserve "1" as "1"
    df = pd.read_csv(uploaded_file, dtype=str)

    # 2) Determine version
    version = determine_version(df)
    if version is None:
        st.error("No version indicator (_v1 or _v2) found in columns.")
        return None

    # 3) Remove existing record_id if any
    if "record_id" in df.columns:
        df.drop(columns=["record_id"], inplace=True)

    # Identify email column
    email_col = next((col for col in df.columns if "email" in col.lower()), None)
    if not email_col:
        st.error("No email column found.")
        return None

    hpi_col = f"historyofpresentillness{version}"
    hpiwords_col = f"hpiwords{version}"
    
    # Check if the history of present illness column exists
    if hpi_col in df.columns:
        df[hpiwords_col] = df[hpi_col].apply(lambda x: len(x.split()) if isinstance(x, str) else 0)
    else:
        st.error(f"Expected column '{hpi_col}' not found.")
        return None

    df["record_id"] = df[email_col].astype(str)

    # Filter out processed
    df = df[~df["record_id"].apply(lambda rid: is_record_processed_version(rid, version))]
    if df.empty:
        st.info("All record_ids have already been processed.")
        return None

    # Move record_id to the front
    df = df[["record_id"] + [c for c in df.columns if c != "record_id"]]

    # Timestamps
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

    # 4) Convert age_{version} to numeric, then map to agex_{version}
    age_col = f"age_{version}"
    if age_col in df.columns:
        df[age_col] = pd.to_numeric(df[age_col], errors="coerce").astype('Int64') 
        df[f"agex_{version}"] = df[age_col].map(age_mapping)

    # 5) Insert line breaks in certain text columns (like physicalexam_{version}, etc.)
    text_cols_to_fix = [
        f"physicalexam_{version}",
        f"vital_signs_and_growth_{version}",
        f"historyofpresentillness_{version}"
        # add more if needed
    ]
    for col in text_cols_to_fix:
        if col in df.columns:
            df[col] = df[col].apply(insert_line_breaks)



    # 6) Build additional columns (like additional_hx_{version}, vital_signs_and_growth_{version})
    build_additional_columns(df, version)

    # 7) Run dynamic AI analysis
    st.info(f"Running AI analysis for {version}... (this may take a while)")
    df[f"notes_2_{version}"]  = df.apply(lambda row: analyze_notes_2(row, version), axis=1)
    df[f"notes_4_{version}"]  = df.apply(lambda row: analyze_notes_4(row, version), axis=1)
    df[f"notes_9_{version}"]  = df.apply(lambda row: analyze_notes_9(row, version), axis=1)
    df[f"notes_12_{version}"] = df.apply(lambda row: analyze_notes_12(row, version), axis=1)
    df[f"notes_15_{version}"] = df.apply(lambda row: analyze_notes_15(row, version), axis=1)

    # 8) Mark processed
    for rid in df["record_id"]:
        mark_record_as_processed_version(rid, version)

    return df, version

########################################
# 9) STREAMLIT UI
########################################

st.title("CSV Processor (Dynamic) with AI & Version-Specific Firestore")

uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
if uploaded_file:
    result = process_file(uploaded_file)
    if result is not None:
        df_processed, version = result
        st.success("File processed successfully!")
        st.dataframe(df_processed)

        # Drop columns like additional_hx_{version}, vital_signs_and_growth_{version}, agex_{version}
        columns_to_remove = [
            f"additional_hx_{version}",
            f"vital_signs_and_growth_{version}",
            f"agex_{version}"
        ]
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
