import pickle

# Test rf_model.pkl
try:
    with open('C:/Users/stakh/Downloads/NIDS/NIDSS/models/rf_model.pkl', 'rb') as f:
        rf_model = pickle.load(f)
    print("RF model loaded successfully")
except Exception as e:
    print(f"Failed to load RF model: {e}")

# Test dt_model.pkl
try:
    with open('C:/Users/stakh/Downloads/NIDS/NIDSS/models/dt_model.pkl', 'rb') as f:
        dt_model = pickle.load(f)
    print("DT model loaded successfully")
except Exception as e:
    print(f"Failed to load DT model: {e}")