import pandas as pd
from sqlalchemy import create_engine

# Database configuration
DATABASE_URL = "sqlite:///C:/Users/poles/Desktop/BiusSulisDolgai/Git_hub_pull/EWMA.db"
TABLE_NAME = "EWMA_Table"

# Adaptive Threshold Parameters
window_size = 50  # Rolling window size (adjust based on periodicity)
n_std_dev = 2  # Number of standard deviations for anomaly detection

# Connect to the database
engine = create_engine(DATABASE_URL)

# Load the existing table into a Pandas DataFrame
print("[INFO] Loading existing data from EWMA.db...")
df = pd.read_sql_table(TABLE_NAME, con=engine)

# Ensure EWMA column exists
if "EWMA" not in df.columns:
    raise ValueError("[ERROR] 'EWMA' column not found in the table.")

# Calculate rolling mean and rolling standard deviation
print("[INFO] Calculating rolling statistics...")
df["Rolling_Mean"] = df["EWMA"].rolling(window=window_size, min_periods=1).mean()
df["Rolling_Std"] = df["EWMA"].rolling(window=window_size, min_periods=1).std()

# Add the Anomaly column
print("[INFO] Identifying anomalies...")
df["Anomaly"] = df.apply(
    lambda row: 1 if (row["EWMA"] > row["Rolling_Mean"] + n_std_dev * row["Rolling_Std"]) or
                 (row["EWMA"] < row["Rolling_Mean"] - n_std_dev * row["Rolling_Std"]) else 0,
    axis=1
)

# Count anomalies
anomaly_count = df["Anomaly"].sum()
print(f"[INFO] Number of anomalies detected: {anomaly_count}")

# Write the updated table back to the database
print("[INFO] Writing updated data back to EWMA.db...")
with engine.connect() as conn:
    df.to_sql(TABLE_NAME, con=conn, if_exists="replace", index=False)

print("[INFO] Update completed. The 'Anomaly' column has been added successfully.")
