import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib
import s3fs

def train_model():
    """
    Trains a Random Forest model to predict fraudulent transactions for each cc_num.
    """
    # S3 paths
    s3_parquet_path = 's3://ece5984-s3-prathamj//Bank_ETL/features/fraud_large_transactions.parquet'
    s3_model_path = 's3://ece5984-s3-prathamj//Bank_ETL/model/enhanced_fraud_detection_model.joblib'
    s3_threshold_path = 's3://ece5984-s3-prathamj//Bank_ETL/model/cc_num_thresholds.csv'

    # Load dataset from S3
    print("Loading data from S3...")
    data = pd.read_parquet(s3_parquet_path, storage_options={'anon': False})

    # Step 1: Calculate user-specific thresholds
    thresholds = data.groupby('cc_num')['amt'].apply(
        lambda x: x.mean() + 3 * x.std()
    ).to_dict()
    data['is_large_transaction'] = data.apply(
        lambda row: 1 if row['amt'] > thresholds[row['cc_num']] else 0, axis=1
    )

    # Step 2: Handle missing values in time_since_last_transaction
    data['time_since_last_transaction'] = data['time_since_last_transaction'].fillna(-1)

    # Step 3: Select relevant features and target
    features = ['amt', 'is_large_transaction', 'time_since_last_transaction']
    target = 'is_fraud'
    data = data[features + [target]]

    # Step 4: Split the data into training and testing sets
    X = data.drop(columns=[target])
    y = data[target]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

    # Step 5: Train a Random Forest model
    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    model.fit(X_train, y_train)

    # Step 6: Evaluate the model
    y_pred = model.predict(X_test)
    print("Classification Report:\n")
    print(classification_report(y_test, y_pred))

    # Step 7: Save the trained model and thresholds to S3
    print("Saving the model and thresholds to S3...")
    fs = s3fs.S3FileSystem(anon=False)
    with fs.open(s3_model_path, 'wb') as f:
        joblib.dump(model, f)
    pd.DataFrame.from_dict(thresholds, orient='index', columns=['threshold']).to_csv(
        s3_threshold_path, storage_options={'anon': False}
    )
    print(f"Model saved to {s3_model_path} and thresholds saved to {s3_threshold_path}.")

if __name__ == '__main__':
    train_model()
