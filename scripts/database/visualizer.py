from sqlalchemy import select
import matplotlib.pyplot as plt
import pandas as pd

from database import DatabaseManager


# Function to query relevant columns directly from the database
def query_columns_from_db(db_manager, column_prefixes):
    """
    Query specific columns from the database based on prefixes.
    
    Args:
        db_manager (DatabaseManager): Database manager instance.
        column_prefixes (list of str): Prefixes to filter column names.
    
    Returns:
        dict: A dictionary where keys are prefixes, and values are lists of matching column names.
    """
    columns = db_manager.model.__table__.columns.keys()
    matching_columns = {prefix: [] for prefix in column_prefixes}
    
    for col in columns:
        for prefix in column_prefixes:
            if col.startswith(prefix):
                matching_columns[prefix].append(col)
    return matching_columns

# Example usage for querying columns and visualizing
if __name__ == "__main__":
    # Initialize DatabaseManager
    DATABASE_URL = "sqlite:///train_test.db"
    
    # Load dataframes
    train_df = pd.read_csv("data/hai_train.csv", sep=";")
    test_df = pd.read_csv("data/hai_test.csv", sep=";")
    
    # Create database managers
    train_db = DatabaseManager(DATABASE_URL, "train", train_df)
    test_db = DatabaseManager(DATABASE_URL, "test", test_df)
    
    # Query the database for relevant columns
    column_prefixes = ["P", "OUT"]
    matching_columns = query_columns_from_db(test_db, column_prefixes)
    
    # Extract columns from matching results
    P_columns = matching_columns["P"][:2]  # First two P columns
    OUT_columns = matching_columns["OUT"]  # All OUT columns
    
    # Query the database for the selected columns, including timestamp
    with test_db.Session() as session:  # Using session context manager
        stmt = select(*[getattr(test_db.model, col) for col in (["timestamp"] + P_columns + OUT_columns)])
        results = session.execute(stmt).fetchall()

    # Convert results to a dictionary for plotting
    data = {col: [] for col in (["timestamp"] + P_columns + OUT_columns)}
    for row in results:
        for col, value in zip(data.keys(), row):
            data[col].append(value)

    # Scatter plot for P columns vs OUT columns
    for P_col in P_columns:
        for OUT_col in OUT_columns:
            plt.figure(figsize=(10, 6))
            plt.scatter(data[P_col], data[OUT_col], alpha=0.7)
            plt.title(f"Scatter Plot: {P_col} vs {OUT_col}")
            plt.xlabel(P_col)
            plt.ylabel(OUT_col)
            plt.grid()
            plt.show()

    # Line plots for P columns over time (ensure 'timestamp' exists)
    if "timestamp" in data:
        for P_col in P_columns:
            plt.figure(figsize=(10, 6))
            
            # Plotting the data
            plt.plot(data["timestamp"], data[P_col], label=P_col, marker="o", color="orange")
            
            # Title and labels
            plt.title(f"Line Plot: {P_col} over Time")
            plt.xlabel("Timestamp")
            plt.ylabel(P_col)
            
            # Rotate the x-axis labels for better readability
            plt.xticks(rotation=45, ha='right')  # Rotate and align right
            
            # Optionally, limit the number of ticks to avoid crowding
            step = max(1, len(data["timestamp"]) // 10)  # Adjust number of labels shown
            plt.xticks(data["timestamp"][::step], rotation=45, ha='right')
            
            # Optionally, format the x-axis labels (if timestamps are datetime objects)
            # If your timestamps are datetime objects, you can format them:
            # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

            # Add grid for clarity
            plt.grid()

            # Add a legend
            plt.legend()
            
            # Show the plot
            plt.tight_layout()  # Ensure labels fit into the figure
            plt.show()