import os
import json
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv
import streamlit as st

# Load environment variables from .env
load_dotenv()

# Define a global variable
FIREBASE_COLLECTION_NAME = os.getenv("FIREBASE_COLLECTION_NAME") or st.secrets.get("FIREBASE_COLLECTION_NAME")

# Initialize Firebase only once
if "firebase_initialized" not in st.session_state:
    firebase_key = os.getenv("FIREBASE_KEY") or st.secrets.get("FIREBASE_KEY")

    if firebase_key is None:
        raise ValueError("❌ FIREBASE_KEY environment variable is missing!")

    try:
        cred = credentials.Certificate(json.loads(firebase_key))
        firebase_admin.initialize_app(cred)
        st.session_state.firebase_initialized = True
    except ValueError as e:
        if "already exists" in str(e):
            st.session_state.firebase_initialized = True
        else:
            st.error(f"🔥 Failed to initialize Firebase: {str(e)}")
            st.stop()

# Firestore client
if "db" not in st.session_state:
    try:
        st.session_state.db = firestore.client()
    except Exception as e:
        st.error(f"🔥 Failed to connect to Firestore: {str(e)}")
        st.stop()

def upload_to_firebase(document_id, entry):
    db = st.session_state.db  # Get Firestore client from session_state
    collection_name = FIREBASE_COLLECTION_NAME

    if collection_name is None:
        raise ValueError("❌ FIREBASE_COLLECTION_NAME is not set.")

    db.collection(collection_name).document(document_id).set(entry, merge=True)
    return "✅ Data uploaded to Firebase."

def load_last_page(document_id):
    db = st.session_state.db
    collection_name = FIREBASE_COLLECTION_NAME
    if document_id:
        user_data = db.collection(collection_name).document(document_id).get()
        if user_data.exists:
            return user_data.to_dict().get("last_page", "welcome")
    return "welcome"

def get_diagnoses_from_firebase(document_id):
    db = st.session_state.db
    collection_name = FIREBASE_COLLECTION_NAME
    doc_ref = db.collection(collection_name).document(document_id)
    user_data = doc_ref.get()
    return user_data.to_dict().get("diagnoses_s1") if user_data.exists else None

