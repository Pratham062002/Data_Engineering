Title: Bank ETL Pipeline for Credit Card Fraud Detection and Visualization

Project function: This project simulates an ETL pipeline to detect and visualize credit card fraud in financial transactions, replicating a bank’s data processing framework. Using the Kaggle Fraud Detection dataset, the pipeline identifies high-risk geographic areas ("red zones") prone to fraudulent activity. The goal is to help bank stakeholders improve fraud prevention strategies by visualizing these areas in Tableau.

Dataset: The dataset used in this project is Kaggle’s Financial Fraud Detection dataset, containing anonymized and labeled transaction records. Key features are transformed using PCA to maintain confidentiality. The ETL pipeline processes the raw data through various transformation layers, such as cleaning, geographic aggregation, anomaly detection, and a final quality assurance (QA) step.

Pipeline / Architecture: The ETL pipeline is structured into the following stages:
1. Extraction: Data is pulled from Kaggle and ingested into AWS S3 for storage.
2. Transformation:
  - Layer 1: Data Cleaning and Preprocessing (handling missing values, formatting inconsistencies).
  - Layer 2: Geographic Aggregation (grouping transactions by location to detect fraud hotspots).
  - Layer 3: Anomaly Detection (using Random Forest to flag fraudulent transactions).
  - Layer 4: QA Transformation (validating data consistency and security).
3. Loading: Transformed data is stored in S3 in Parquet format for optimized querying.
4. Visualization: A Tableau dashboard visualizes fraud hotspots, displaying areas with elevated fraud risks. 

Data Quality Assessment:
The dataset is mostly clean, with no missing or duplicate records, as confirmed through exploratory data analysis (EDA). Additional preprocessing involved:
-  Data Type Standardization: Ensuring date and timestamp columns adhere to banking standards.
-  Outlier Detection: Flagging transactions as anomalous if they exceed three standard deviations from the mean. Further steps include scaling numerical features and encoding categorical variables.

Data Transformation Models used:
Key transformations included:
-  Date Formatting: Standardized to yyyyMMdd for dates and yyyy-MM-dd HH:mm:ss for timestamps.
-  Feature Engineering: Added calculated columns like time_since_last_transaction and is_large_transaction to capture important transaction patterns.
-  Random Forest Classifier: Used to predict fraudulent transactions, with outputs saved as pickle files (fraud_report and city_report) for further visualization. Parquet files storing transformed data include             
   fraud_large_transactions.parquet, used for training the model.
   
Infographic:

