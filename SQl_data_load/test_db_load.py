import joblib
import numpy as np

# Load the .pkl file
data = joblib.load("SQl_data_load/clean_aapl.pkl")

# Print the numpy version in the current environment
print(f"Numpy version: {np.__version__}")