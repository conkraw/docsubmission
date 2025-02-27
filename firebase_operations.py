import os
import json
import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv
import streamlit as st

# Load environment variables if running locally
load_dotenv()

# Load Firebase credentials
FIREBASE_COLLECTION = os.getenv("FIREBASE_COLLECTION") or st.secrets.get("FIREBASE_COLLECTION")
FIREBASE_KEY = os.getenv("FIREBASE_KEY") or st.secrets.get("firebase")

if FIREBASE_KEY is None:
    raise ValueError("❌ FIREBASE_KEY is missing! Ensure it is set in .env or Streamlit Secrets.")

try:
    # Ensure private_key is correctly formatted
    firebase_creds = json.loads(FIREBASE_KEY.replace("\\n", "\n"))

    # Initialize Firebase only once
    if not firebase_admin._apps:
        cred = credentials.Certificate(firebase_creds)
        firebase_admin.initialize_app(cred)
        st.session_state["firebase_initialized"] = True

except Exception as e:
    st.error(f"🔥 Firebase initialization failed: {e}")
    st.stop()

# Firestore client
if "db" not in st.session_state:
    try:
        st.session_state["db"] = firestore.client()
    except Exception as e:
        st.error(f"🔥 Failed to connect to Firestore: {e}")
        st.stop()


