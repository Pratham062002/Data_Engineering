import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs  # Required for saving files to S3

# Define the S3 bucket and folder
S3_BUCKET = 's3://ece5984-s3-prathamj'
S3_FOLDER = '/Bank_ETL/features/'

def transform_data():
    # Load the CSV file containing the raw data
    raw_data = pd.read_csv('s3://ece5984-s3-prathamj/Bank_ETL/fraudTrain.csv')    

    # Transforming 'trans_date_trans_time' column to standard format for banking
    if 'trans_date_trans_time' in raw_data.columns:
        raw_data['trans_date_trans_time'] = pd.to_datetime(raw_data['trans_date_trans_time'])  # Ensure datetime format
        raw_data['date'] = raw_data['trans_date_trans_time'].dt.strftime('%Y%m%d')  # Convert to yyyyMMdd
        raw_data['timestamp'] = raw_data['trans_date_trans_time'].dt.strftime('%Y-%m-%d %H:%M:%S')  # Convert to yyyy-MM-dd HH:mm:ss

    # Calculate the time difference between consecutive transactions for each credit card in minutes
    raw_data = raw_data.sort_values(by=['cc_num', 'trans_date_trans_time'])  # Sort by cc_num and transaction time
    raw_data['time_since_last_transaction'] = raw_data.groupby('cc_num')['trans_date_trans_time'].diff().dt.total_seconds() / 60  # Convert seconds to minutes

    # Flag transactions that are 3 standard deviations above the mean
    threshold = raw_data['amt'].mean() + 3 * raw_data['amt'].std()
    raw_data['is_large_transaction'] = raw_data['amt'] > threshold

    # Filter transactions with small time differences (less than 5 minutes); this could be fraud as well.
    short_interval_transactions = raw_data[raw_data['time_since_last_transaction'] < 5].copy()

    # Spending Spike Flag (adjusted threshold)
    # Using the 90th percentile to detect spending spikes
    spending_spike_threshold = raw_data['amt'].quantile(0.85)
    raw_data['spending_spike_flag'] = (raw_data['amt'] > spending_spike_threshold).astype(int)

    # Initialize S3 filesystem
    s3 = s3fs.S3FileSystem()

    # Save transformed data to S3
    test_data = raw_data.head(100)  # Use 100 records as an example for testing
    test_path = f's3://{S3_BUCKET}/{S3_FOLDER}fraud_transformed_test.parquet'
    pq.write_table(pa.Table.from_pandas(test_data), test_path, filesystem=s3)

    # Save short interval transactions to S3
    short_interval_path = f's3://{S3_BUCKET}/{S3_FOLDER}fraud_short_interval_transactions.parquet'
    pq.write_table(pa.Table.from_pandas(short_interval_transactions), short_interval_path, filesystem=s3)

    # Save large transactions to S3
    large_transactions = raw_data[raw_data['is_large_transaction']].copy()
    large_transactions_path = f's3://{S3_BUCKET}/{S3_FOLDER}fraud_large_transactions.parquet'
    pq.write_table(pa.Table.from_pandas(large_transactions), large_transactions_path, filesystem=s3)

    # Save transactions with spending spikes to S3
    spending_spike_transactions = raw_data[raw_data['spending_spike_flag'] == 1].copy()
    spending_spike_path = f's3://{S3_BUCKET}/{S3_FOLDER}fraud_spending_spike_transactions.parquet'
    pq.write_table(pa.Table.from_pandas(spending_spike_transactions), spending_spike_path, filesystem=s3)

    print("Data transformation complete. Files saved to S3.")

