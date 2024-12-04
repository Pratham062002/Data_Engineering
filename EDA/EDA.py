import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the dataset
data = pd.read_csv('fraudTrain.csv')

# Basic Summary EDA

# Display basic information about the dataset
print("Basic Information:")
print(data.info())
print("====================================")

# Summary Statistics
print("Summary Statistics:")
print(data.describe().to_string())
print("====================================")

# Check for missing values
print("Missing Values in Each Column:")
print(data.isnull().sum())
print("====================================")

# Fraud Distribution
fraud_counts = data['is_fraud'].value_counts()
fraud_percentage = (fraud_counts[1] / fraud_counts.sum()) * 100
print(f"Fraud Distribution: {fraud_counts.to_dict()}")
print(f"Fraud Percentage: {fraud_percentage:.2f}%")
print("====================================")

# Summary statistics for transaction amounts (for fraud and non-fraud)
print("Summary Statistics for Fraudulent Transactions:")
print(data[data['is_fraud'] == 1]['amt'].describe())
print("====================================")
print("Summary Statistics for Non-Fraudulent Transactions:")
print(data[data['is_fraud'] == 0]['amt'].describe())
print("====================================")

# Time-Based Analysis

if data['trans_date_trans_time'].dtype == 'object':
    data['trans_date_trans_time'] = pd.to_datetime(data['trans_date_trans_time'])

# Extract time-based features
data['hour'] = data['trans_date_trans_time'].dt.hour
data['day_of_week'] = data['trans_date_trans_time'].dt.day_name()
data['month'] = data['trans_date_trans_time'].dt.month

# Setting up color palette for fraud (red) vs. non-fraud (blue)
palette = {0: 'skyblue', 1: 'salmon'}

# Plotting time-based and geographical analysis in a 2x2 layout
fig, axs = plt.subplots(2, 2, figsize=(16, 12))

# Transactions by hour of the day (scaled log)
sns.countplot(x='hour', data=data, hue='is_fraud', palette=palette, ax=axs[0, 0])
axs[0, 0].set_title('Transactions by Hour of the Day (Fraud vs. Non-Fraud)', fontsize=14)
axs[0, 0].set_xlabel('Hour of the Day', fontsize=12)
axs[0, 0].set_ylabel('Transaction Count (Log Scale)', fontsize=12)
axs[0, 0].set_yscale('log')
axs[0, 0].legend(title='Fraud Status', labels=['Non-Fraud', 'Fraud'], fontsize=10)

# Transactions by day of the week (scaled log)
sns.countplot(x='day_of_week', data=data, hue='is_fraud', palette=palette, 
              order=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ax=axs[0, 1])
axs[0, 1].set_title('Transactions by Day of the Week (Fraud vs. Non-Fraud)', fontsize=14)
axs[0, 1].set_xlabel('Day of the Week', fontsize=12)
axs[0, 1].set_ylabel('Transaction Count (Log Scale)', fontsize=12)
axs[0, 1].set_yscale('log')
axs[0, 1].legend(title='Fraud Status', labels=['Non-Fraud', 'Fraud'], fontsize=10)
axs[0, 1].tick_params(axis='x', rotation=45)

# Transactions by month (scaled log)
sns.countplot(x='month', data=data, hue='is_fraud', palette=palette, ax=axs[1, 0])
axs[1, 0].set_title('Transactions by Month (Fraud vs. Non-Fraud)', fontsize=14)
axs[1, 0].set_xlabel('Month', fontsize=12)
axs[1, 0].set_ylabel('Transaction Count (Log Scale)', fontsize=12)
axs[1, 0].set_yscale('log')
axs[1, 0].legend(title='Fraud Status', labels=['Non-Fraud', 'Fraud'], fontsize=10)

# Geographical Analysis - Top 10 cities with the most fraudulent transactions
# Assuming 'city' and 'state' columns exist
fraud_geo = data[data['is_fraud'] == 1].groupby(['state', 'city']).size().reset_index(name='fraud_count')
fraud_geo = fraud_geo.sort_values(by='fraud_count', ascending=False).head(10)

sns.barplot(x='fraud_count', y='city', data=fraud_geo, palette="viridis", ax=axs[1, 1])
axs[1, 1].set_title('Top 10 Cities with Most Fraudulent Transactions', fontsize=14)
axs[1, 1].set_xlabel('Number of Fraudulent Transactions', fontsize=12)
axs[1, 1].set_ylabel('City', fontsize=12)

# Adding data labels to the top 10 cities' fraud count bars
for index, value in enumerate(fraud_geo['fraud_count']):
    axs[1, 1].text(value + 0.5, index, str(value), color='black', va="center", fontsize=10)

# Adjust layout for better spacing
plt.tight_layout(pad=2.0)
plt.show()

