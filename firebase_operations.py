# firebase_operations.py
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json

# Define a global variable
FIREBASE_COLLECTION_NAME = None

# Initialize Firebase
def initialize_firebase():
    global FIREBASE_COLLECTION_NAME  # Use the global variable

    FIREBASE_KEY_JSON = os.getenv('FIREBASE_KEY')
    FIREBASE_COLLECTION_NAME = os.getenv('FIREBASE_COLLECTION_NAME')
    
    if FIREBASE_KEY_JSON is None:
        raise ValueError("FIREBASE_KEY environment variable not set.")

    try:
        firebase_credentials = json.loads(FIREBASE_KEY_JSON)

        if not firebase_admin._apps:
            cred = credentials.Certificate(firebase_credentials)
            firebase_admin.initialize_app(cred)

        return firestore.client()
    except Exception as e:
        raise Exception(f"Error initializing Firebase: {e}")

def upload_to_firebase(db, document_id, entry):
    global FIREBASE_COLLECTION_NAME  # Access the global variable
    
    if FIREBASE_COLLECTION_NAME is None:
        raise ValueError("FIREBASE_COLLECTION_NAME is not set.")
    
    db.collection(FIREBASE_COLLECTION_NAME).document(document_id).set(entry, merge=True) 
    return "Data uploaded to Firebase."

# utils/firebase_operations.py

def load_last_page(db, document_id):
    collection_name = st.secrets["FIREBASE_COLLECTION_NAME"]  # Get collection name from secrets
    
    # Check if the document ID exists in the database
    if document_id:
        user_data = db.collection(collection_name).document(document_id).get()
        if user_data.exists:
            return user_data.to_dict().get("last_page")  # Return the last_page if found
    return "welcome"  # Default to 'welcome' if no last_page is found

# Example function to retrieve diagnoses from Firebase

def get_diagnoses_from_firebase(db, document_id):
    collection_name = st.secrets["FIREBASE_COLLECTION_NAME"]
    doc_ref = db.collection(collection_name).document(document_id)
    
    # Get the document from Firebase
    user_data = doc_ref.get()
    
    if user_data.exists:
        # Retrieve the diagnoses data (if it exists)
        diagnoses = user_data.to_dict().get("diagnoses_s1", None)
        return diagnoses  # Return the stored diagnoses data
    return None  # No data found

