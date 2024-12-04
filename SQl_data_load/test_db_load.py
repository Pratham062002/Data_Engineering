import joblib
import pandas as pd

def test_local_files():
    # Local file paths
    fraud_report_path = "fraud_report_with_cities.pkl"
    city_report_path = "city_fraud_report.pkl"

    try:
        # Load fraud report file
        fraud_report_with_cities = joblib.load(fraud_report_path)
        print("Loaded fraud_report_with_cities.pkl successfully")
        print(fraud_report_with_cities.head())

        # Load city report file
        city_fraud_report = joblib.load(city_report_path)
        print("Loaded city_fraud_report.pkl successfully")
        print(city_fraud_report.head())

    except Exception as e:
        print(f"Error loading files: {e}")

if __name__ == "__main__":
    test_local_files()
