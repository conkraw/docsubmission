import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase if not already initialized
if not firebase_admin._apps:
    # st.secrets["firebase_service_account"] is automatically loaded as a dict from secrets.toml
    cred = credentials.Certificate(st.secrets["firebase_service_account"])
    firebase_admin.initialize_app(cred)

# Get a Firestore client
db = firestore.client()

st.title("Firestore Communication Test")

# Write a test document to Firestore
doc_ref = db.collection("test").document("testDoc")
doc_ref.set({"message": "Hello from Streamlit!"})

st.write("Test document written to Firestore.")

# Read the test document from Firestore
doc = doc_ref.get()
if doc.exists:
    st.write("Document data:", doc.to_dict())
else:
    st.write("No such document found!")
