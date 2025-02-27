import os
import json
import firebase_admin
from firebase_admin import credentials, firestore

# Global variable for Firestore collection
FIREBASE_COLLECTION_NAME = None

# Initialize Firebase
def initialize_firebase():
    global FIREBASE_COLLECTION_NAME  # Use global variable

    # Get Firebase credentials and collection name
    FIREBASE_KEY_JSON = os.getenv("FIREBASE_KEY") or st.secrets.get("FIREBASE_KEY")
    FIREBASE_COLLECTION_NAME = os.getenv("FIREBASE_COLLECTION_NAME") or st.secrets.get("FIREBASE_COLLECTION_NAME")

    if FIREBASE_KEY_JSON is None:
        raise ValueError("❌ FIREBASE_KEY environment variable is not set. Add it to .env or Streamlit secrets.")

    try:
        # Parse Firebase JSON credentials
        firebase_credentials = json.loads(FIREBASE_KEY_JSON)

        # Initialize Firebase if it's not already initialized
        if not firebase_admin._apps:
            cred = credentials.Certificate(firebase_credentials)
            firebase_admin.initialize_app(cred)

        return firestore.client()  # Return Firestore client
    except Exception as e:
        raise Exception(f"🔥 Error initializing Firebase: {e}")

# Function to upload data to Firestore
def upload_to_firebase(db, document_id, entry):
    global FIREBASE_COLLECTION_NAME
    
    if FIREBASE_COLLECTION_NAME is None:
        raise ValueError("❌ FIREBASE_COLLECTION_NAME is not set.")

    try:
        db.collection(FIREBASE_COLLECTION_NAME).document(document_id).set(entry, merge=True)
        return f"✅ Data uploaded to Firestore: {document_id}"
    except Exception as e:
        raise Exception(f"🔥 Error uploading to Firestore: {e}")

# Function to fetch processed records from Firestore
def fetch_processed_records(db):
    global FIREBASE_COLLECTION_NAME

    if FIREBASE_COLLECTION_NAME is None:
        raise ValueError("❌ FIREBASE_COLLECTION_NAME is not set.")

    try:
        records_ref = db.collection(FIREBASE_COLLECTION_NAME).stream()
        records = {doc.id: doc.to_dict() for doc in records_ref}
        return records if records else {}
    except Exception as e:
        raise Exception(f"🔥 Error fetching records from Firestore: {e}")
