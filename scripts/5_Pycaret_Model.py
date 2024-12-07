### Pycaret unsupervised model ###
# pip install pycaret[time_series]

## IT WAS DONE IN GOOGLE COLAB ##

# Import required libraries
import pandas as pd
from pycaret.anomaly import setup, create_model, assign_model, save_model
import os
import matplotlib.pyplot as plt
import joblib
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

# File paths
data_dir = "/content/drive/MyDrive/OST Stream Mining/"
dataset_file = os.path.join(data_dir, "hai-train1.csv")

# Load dataset
data = pd.read_csv(dataset_file, delimiter=";")

# Ensure the timestamp column is parsed as a datetime object
data['timestamp'] = pd.to_datetime(data['timestamp'])

# Drop duplicate indices by resetting the index
data.reset_index(drop=True, inplace=True)

# Select relevant features for anomaly detection
selected_features = ["P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT"]
data = data[selected_features]

# PyCaret Anomaly Detection Setup
exp_name = setup(data=data, index=False)  # Reset the index to RangeIndex during setup

# Create an Isolation Forest model
model = create_model('iforest')  # You can also try 'knn', 'hdbscan', or other models

# Assign anomaly labels to the dataset
results = assign_model(model)

# Save the dataset with anomalies
results_file = os.path.join(data_dir, "train_with_anomalies_pycaret.csv")
results.to_csv(results_file)
print(f"Anomalies detected and saved to {results_file}")

# Save the model for future use
model_path = os.path.join(data_dir, "iforest_model_pycaret")
save_model(model, model_path)
print(f"Model saved at: {model_path}")


#####################
### Visualiaztion ###
#####################



# File paths
original_file = r"/content/drive/MyDrive/OST Stream Mining/hai-train1.csv"
results_file = r"/content/drive/MyDrive/OST Stream Mining/train_with_anomalies_pycaret.csv"

# Load the original dataset and the results
original_data = pd.read_csv(original_file, delimiter=";")
results = pd.read_csv(results_file)

# Ensure the timestamp column is parsed as datetime
original_data['timestamp'] = pd.to_datetime(original_data['timestamp'])

# Add the timestamp back to the results DataFrame
results['timestamp'] = original_data['timestamp']

# Select a feature to visualize
feature_to_plot = "P1_FCV01D"

# Plot the feature with anomalies highlighted
plt.figure(figsize=(12, 6))
plt.plot(results['timestamp'], results[feature_to_plot], label=f'{feature_to_plot} (Normal)', alpha=0.7)
anomalies = results[results['Anomaly'] == 1]
plt.scatter(
    anomalies['timestamp'],
    anomalies[feature_to_plot],
    color='red',
    label='Anomalies',
    zorder=5
)

# Add labels and title
plt.xlabel('Timestamp')
plt.ylabel(feature_to_plot)
plt.title(f"{feature_to_plot} with Highlighted Anomalies")
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()


#####################
###  Evaulation   ###
#####################

# Note: It doesn't penalize if the model predicts everything normal (0 attack label) - so it shows really good metrics, altough it didn't detect anything

# Paths to the test file and the saved model
test_file_path = "/content/drive/MyDrive/OST Stream Mining/hai-test1_with_label.csv"
model_path = "/content/drive/MyDrive/OST Stream Mining/iforest_model_pycaret.pkl"

# Load the test dataset
test_data = pd.read_csv(test_file_path, delimiter=";")

# Parse timestamps and set as index
test_data['timestamp'] = pd.to_datetime(test_data['timestamp'])
test_data.set_index('timestamp', inplace=True)

# Load the saved isolation forest model
model = joblib.load(model_path)

# Select the same features used for training
selected_features = ["P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT"]
test_features = test_data[selected_features]

# Predict anomalies (-1 = anomaly, 1 = normal)
test_data['Anomaly_Score'] = model.decision_function(test_features)
test_data['Anomaly'] = model.predict(test_features)

# Map model predictions to 1 for anomalies, 0 for normal
test_data['Anomaly'] = (test_data['Anomaly'] == -1).astype(int)

# Ensure attack_label is an integer
test_data['attack_label'] = test_data['attack_label'].astype(int)

# Evaluate predictions against ground truth
conf_matrix = confusion_matrix(test_data['attack_label'], test_data['Anomaly'])
class_report = classification_report(test_data['attack_label'], test_data['Anomaly'], digits=4)
accuracy = accuracy_score(test_data['attack_label'], test_data['Anomaly'])

print("Confusion Matrix:")
print(conf_matrix)

print("\nClassification Report:")
print(class_report)

print(f"Accuracy: {accuracy:.4f}")

# Save the test results with predictions
test_data.to_csv("/content/drive/MyDrive/OST Stream Mining/test_with_predictions.csv")
print("Test results with predictions saved to '/content/drive/MyDrive/OST Stream Mining/test_with_predictions.csv'")


