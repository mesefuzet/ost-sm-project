import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker

# Define the declarative base and global database URL
Base = declarative_base()


class DatabaseManager:
    def __init__(self, db_url: str, table_name: str, df: pd.DataFrame):
        """
        Initialize the database manager.
        
        Args:
            db_url (str): Database connection string.
            table_name (str): Name of the table.
            df (pd.DataFrame): DataFrame to define the schema.
        """
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.table_name = table_name
        self.df = df
        self.model = self._create_table_class()

        # Ensure the table is created
        Base.metadata.create_all(self.engine)

    def _infer_sqlalchemy_columns(self):
        """Infer SQLAlchemy column types from the DataFrame."""
        column_types = {}
        for col, dtype in self.df.dtypes.items():
            if dtype == 'int64':
                column_types[col] = Column(Integer)
            elif dtype == 'float64':
                column_types[col] = Column(Float)
            elif dtype == 'object':
                column_types[col] = Column(String)
            else:
                raise ValueError(f"Unsupported dtype: {dtype}")
        return column_types

    def _create_table_class(self):
        """Dynamically create a table class based on the DataFrame schema."""
        columns = self._infer_sqlalchemy_columns()
        columns['id'] = Column(Integer, primary_key=True, autoincrement=True)  # Add primary key
        columns['__tablename__'] = self.table_name  # Add table name
        return type(self.table_name, (Base,), columns)

    def insert(self, data: dict):
        """Insert a single record into the table."""
        session = self.Session()
        try:
            session.add(self.model(**data))
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def bulk_insert(self, data: pd.DataFrame):
        """Insert multiple records from a DataFrame."""
        session = self.Session()
        try:
            objects = [self.model(**row.to_dict()) for _, row in data.iterrows()]
            session.bulk_save_objects(objects)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def query(self, filters=None, order_by=None, limit=None):
        """Query records from the table."""
        session = self.Session()
        try:
            query = session.query(self.model)
            if filters:
                query = query.filter(*filters)
            if order_by:
                query = query.order_by(order_by)
            if limit:
                query = query.limit(limit)
            return query.all()
        finally:
            session.close()

    def delete(self, filters):
        """Delete records from the table."""
        session = self.Session()
        try:
            query = session.query(self.model).filter(*filters)
            query.delete(synchronize_session=False)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def load_all(self):
        """Load all records from the table."""
        session = self.Session()
        try:
            return session.query(self.model).all()
        finally:
            session.close()

    def save_to_disk(self, path: str):
        """Save the table's data to a CSV file."""
        records = self.load_all()
        data = pd.DataFrame([record.__dict__ for record in records])
        data.drop(columns=["_sa_instance_state"], inplace=True)  # Remove SQLAlchemy internal state
        data.to_csv(path, index=False)

    def load_from_disk(self, path: str):
        """Load data from a CSV file and insert into the table."""
        data = pd.read_csv(path)
        self.bulk_insert(data)

# Example Usage
if __name__ == "__main__":
    # Define the database path
    DATABASE_URL = "sqlite:///train_test.db"

    # Load dataframes
    print("Loading dataframes to be added to the database...")
    train_df = pd.read_csv("data/hai_train.csv", sep=";")
    test_df = pd.read_csv("data/hai_test.csv", sep=";")

    # Create database managersa
    print("Creating database managers...")
    train_db = DatabaseManager(DATABASE_URL, "train", train_df)
    test_db = DatabaseManager(DATABASE_URL, "test", test_df)

    # Bulk insert data into databases
    print("Inserting data into the databases...")
    train_db.bulk_insert(train_df)
    test_db.bulk_insert(test_df)

    # Query some data
    print("Querying a few samples form the data...")
    results = train_db.query(limit=5)
    for record in results:
        print(record.__dict__)

    # Save to disk
    print("Saving to disk...")
    train_db.save_to_disk("data/train_backup.csv")
    test_db.save_to_disk("data/test_backup.csv")

    # Reload from disk
    print("Reloading from disk...")
    train_db.load_from_disk("data/train_backup.csv")
    test_db.load_from_disk("data/test_backup.csv")