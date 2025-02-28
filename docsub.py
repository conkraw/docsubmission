import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore

# Check that the secret is loaded as a dict
st.write("Type of firebase_service_account:", type(st.secrets["firebase_service_account"]))

# Initialize Firebase only if not already initialized
if not firebase_admin._apps:
    cred = credentials.Certificate(st.secrets["firebase_service_account"])
    firebase_admin.initialize_app(cred)

# Get a Firestore client
db = firestore.client()

st.title("Firestore Communication Test")

# Write a test document
doc_ref = db.collection("test").document("testDoc")
doc_ref.set({"message": "Hello from Streamlit!"})

st.write("Test document written to Firestore.")

# Read the test document back
doc = doc_ref.get()
if doc.exists:
    st.write("Document data:", doc.to_dict())
else:
    st.write("No such document found!")
