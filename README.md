# Bank ETL Pipeline for Credit Card Fraud Detection and Visualization

## Project Function
This project simulates an ETL (Extract, Transform, Load) pipeline designed to detect and visualize credit card fraud in financial transactions, replicating a bank’s data processing framework. Using the Kaggle Fraud Detection dataset, the pipeline identifies high-risk geographic areas ("red zones") prone to fraudulent activity. The goal is to help bank stakeholders improve fraud prevention strategies by visualizing these areas in Tableau.

---

## Dataset
The dataset used in this project is Kaggle’s Financial Fraud Detection dataset, containing anonymized and labeled transaction records. Key features are transformed using PCA to maintain confidentiality. The ETL pipeline processes the raw data through various transformation layers, such as cleaning, geographic aggregation, anomaly detection, and a final quality assurance (QA) step.

---

## ETL Pipeline / Architecture
The ETL pipeline is structured into the following stages:

### 1. Extraction
- Data is pulled from Kaggle and ingested into AWS S3 for storage.

### 2. Transformation
- **Layer 1: Data Cleaning and Preprocessing**
  - Handling missing values and formatting inconsistencies.

- **Layer 2: Geographic Aggregation**
  - Grouping transactions by location to detect fraud hotspots.

- **Layer 3: Anomaly Detection**
  - Using a Random Forest classifier to flag fraudulent transactions.

- **Layer 4: QA Transformation**
  - Validating data consistency and security.

### 3. Loading
- Transformed data is stored in S3 in Parquet format for optimized querying.

### 4. Visualization
- A Tableau dashboard visualizes fraud hotspots, displaying areas with elevated fraud risks.

---

## Data Quality Assessment
The dataset is mostly clean, with no missing or duplicate records, as confirmed through exploratory data analysis (EDA). Additional preprocessing involved:

- **Data Type Standardization:** Ensuring date and timestamp columns adhere to banking standards.
- **Outlier Detection:** Flagging transactions as anomalous if they exceed three standard deviations from the mean.
- **Scaling and Encoding:** Scaling numerical features and encoding categorical variables.

---

## Data Transformation Models Used
Key transformations included:

- **Date Formatting:** Standardized to `yyyyMMdd` for dates and `yyyy-MM-dd HH:mm:ss` for timestamps.
- **Feature Engineering:** Added calculated columns like `time_since_last_transaction` and `is_large_transaction` to capture important transaction patterns.
- **Random Forest Classifier:** Used to predict fraudulent transactions. Outputs are saved as pickle files (`fraud_report` and `city_report`) for further visualization.
- **Parquet Files:** Transformed data is stored in Parquet format, including `fraud_large_transactions.parquet` for model training and visualization purposes along with the predicted fraud tables in pkl format.

---

## Infographic

**ETL Pipeline:**

![Bank_ETL_flowchart](https://github.com/user-attachments/assets/4ae5ed16-7b8b-405a-88b0-77ecc6666de1)

**Result Visualization:**

![Bank Dashboard](https://github.com/user-attachments/assets/9962b81b-3f93-4545-9db1-167640dd0447)

---

Code: 

---

### Thorough Investigation
This project demonstrates the ability to predict fraudulent transactions and strong scalability with its cloud-native design using AWS S3 and a modular ETL pipeline. Enhancements like user-specific features, geographic aggregation, and Tableau visualizations make it impactful for stakeholders seeking actionable insights.

To scale further, transitioning to real-time fraud detection using tools like Apache Kafka and implementing periodic retraining of the Random Forest model are recommended. While the current project requires manual intervention for batch updates and model retraining, automating these processes through a fully integrated MLOps pipeline would significantly improve efficiency and reliability.

Additionally, strengthening compliance with regulations such as GDPR and scaling the infrastructure to accommodate larger datasets and live transaction streams would solidify this pipeline as a robust, innovative solution for fraud detection. These enhancements position the project as a forward-thinking initiative that bridges machine learning and data engineering for effective fraud prevention.
