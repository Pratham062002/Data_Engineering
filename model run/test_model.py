import pandas as pd
import joblib
import s3fs

def test_model():
    """
    Tests the enhanced fraud detection model and generates fraud reports saved to S3.
    """
    # S3 paths
    s3_parquet_path = 's3://ece5984-s3-prathamj//Bank_ETL/features/fraud_large_transactions.parquet'
    s3_model_path = 's3://ece5984-s3-prathamj//Bank_ETL/model/enhanced_fraud_detection_model.joblib'
    s3_threshold_path = 's3://ece5984-s3-prathamj//Bank_ETL/model/cc_num_thresholds.csv'
    s3_city_report_path = 's3://ece5984-s3-prathamj//Bank_ETL/reports/city_fraud_report.pkl'
    s3_fraud_report_path = 's3://ece5984-s3-prathamj//Bank_ETL/reports/fraud_report_with_cities.pkl'

    # Load dataset and model from S3
    print("Loading data and model from S3...")
    fs = s3fs.S3FileSystem(anon=False)
    data = pd.read_parquet(s3_parquet_path, storage_options={'anon': False})
    with fs.open(s3_model_path, 'rb') as f:
        model = joblib.load(f)
    thresholds = pd.read_csv(s3_threshold_path, index_col=0, storage_options={'anon': False}).to_dict()['threshold']

    # Step 1: Recalculate user-specific 'is_large_transaction' based on thresholds
    data['is_large_transaction'] = data.apply(
        lambda row: 1 if row['amt'] > thresholds[row['cc_num']] else 0, axis=1
    )

    # Step 2: Handle missing values in 'time_since_last_transaction'
    data['time_since_last_transaction'] = data['time_since_last_transaction'].fillna(-1)

    # Step 3: Predict fraud for each transaction
    features = ['amt', 'is_large_transaction', 'time_since_last_transaction']
    X = data[features]
    data['fraud_prediction'] = model.predict(X)

    # Step 4: Generate fraud reports
    fraud_transactions = data[data['fraud_prediction'] == 1]
    fraud_report = fraud_transactions.groupby(['cc_num', 'city']).size().reset_index(name='fraud_count')
    city_report = fraud_transactions.groupby('city').size().reset_index(name='fraud_count')

    # Step 5: Save fraud reports to S3 as Pickle
    print("Saving fraud reports to S3...")
    fraud_report.to_pickle(s3_fraud_report_path, storage_options={'anon': False})
    city_report.to_pickle(s3_city_report_path, storage_options={'anon': False})
    print(f"Fraud reports saved to {s3_fraud_report_path} and {s3_city_report_path}.")

if __name__ == '__main__':
    test_model()
