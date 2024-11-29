import json
import pandas as pd

from sklearn.metrics import classification_report, confusion_matrix, precision_score, recall_score, f1_score


def process(path): 
	# Load predictions
	predictions = pd.read_csv(path)

	# Assuming the CSV has columns: 'timestamp', 'ground_truth', 'prediction'
	ground_truth = predictions['ground_truth']  # Actual labels
	prediction = predictions['prediction']  # Model predictions

	# Evaluate the model
	report = classification_report(
		ground_truth, prediction, 
		target_names=["Normal", "Anomaly"], 
		output_dict=True
	)

	# Confusion Matrix
	conf_matrix = confusion_matrix(ground_truth, prediction)

	# calculate scores 
	precision = precision_score(ground_truth, prediction)
	recall = recall_score(ground_truth, prediction)
	f1 = f1_score(ground_truth, prediction)

	return {
        "report": report,
        "confusion_matrix": conf_matrix,
        "precision": precision,
        "recall": recall,
        "f1_score": f1
   }

if __name__ == "__main__": 
	data_path = "models/predictions.csv"
	print(
		json.dumps(
			process(data_path),
			indent=4
		)
	)