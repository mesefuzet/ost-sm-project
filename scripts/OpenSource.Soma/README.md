# Anomaly Detection Project

This project demonstrates how to use KMeans clustering for anomaly detection with **train** and **test** data stored in a SQLite database. The main functionality of the project is contained in the Jupyter notebook `anomaly_predictor.ipynb`, which uses the dataset to predict anomalies. Additionally, Docker is used to containerize the application for easy setup and distribution.

## Project Structure

The project contains the following files and directories:

```
.
├── Dockerfile               # Dockerfile to build the container for the application
├── docker-compose.yml       # Docker Compose configuration file
├── data.db                 # SQLite database containing train and test data
├── anomaly_predictor.ipynb  # Jupyter notebook for anomaly prediction using KMeans
├── grafana-dashboard/       # Folder containing Grafana dashboard JSON files
│   └── grafana_dashboard.json
├── requirements.txt         # Python dependencies for the Jupyter notebook
└── README.md                # This README file
```

## Prerequisites

Before you start, ensure you have the following installed:

- **Docker**: To build and run containers for Grafana and the application.
- **Docker Compose**: To manage multi-container applications.
- **Python 3.x**: Required for the Jupyter notebook (`anomaly_predictor.ipynb`).
- **Jupyter Notebook**: Required to run and execute the notebook.

## Setup Instructions

### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/yourusername/anomaly-detection.git
cd anomaly-detection
```

### 2. Build and Run Docker Containers

To simplify the setup, this project uses **Docker** and **Docker Compose** for containerization. Follow these steps to build and start the services:

#### **Step 1: Build the Docker Containers**

To build the Docker containers, run:

```bash
docker compose build
```

#### **Step 2: Start the Containers**

Once the build is complete, start the containers:

```bash
docker compose up -d
```

This will start both the **Grafana** and **SQLite** services. You can access Grafana at `http://localhost:3000` with the default username and password (`admin` / `admin`).

### 3. Run the Jupyter Notebook

The `anomaly_predictor.ipynb` file is a Jupyter notebook used for anomaly detection using KMeans clustering. To run the notebook, follow these steps:

#### **Step 1: Install Dependencies**

Install the Python dependencies listed in the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

#### **Step 2: Start Jupyter Notebook**

Start Jupyter Notebook:

```bash
jupyter notebook anomaly_predictor.ipynb
```

The notebook will open in your default web browser. Follow the instructions in the notebook to run the KMeans clustering and anomaly prediction.

### 4. Accessing and Importing Grafana Dashboards

The Grafana dashboard is pre-configured to visualize the data. To access and import the dashboard:

1. Open Grafana at `http://localhost:3000`.
2. Login with the default credentials: **Username**: `admin`, **Password**: `admin`.
3. Navigate to the **Dashboard** section and import the pre-configured dashboard.
   - If the dashboard JSON file is hosted on GitHub, you can import it directly from the **Grafana settings** page, or you can download the `grafana_dashboard.json` file and upload it manually.

### 5. Database Information

The project uses a **SQLite database** (`data.db`) to store the **train** and **test** data used for anomaly prediction. You can query and analyze the data using the following tables:

- **train**: Contains training data for anomaly detection.
- **test**: Contains test data, which also includes the `attack_label` used for validation of the anomaly detection.

You can query the data using SQL in Grafana or in a Docker container running the SQLite database:

```bash
docker exec -it sqlite-container sqlite3 /data/data.db "SELECT timestamp, P1_FCV01D FROM train LIMIT 10;"
```

## How the Anomaly Detection Works

1. The Jupyter notebook `anomaly_predictor.ipynb` loads the train and test data from the SQLite database.
2. The notebook preprocesses the data and applies KMeans clustering to the train data.
3. Anomalies are detected in the test data based on the predictions from the KMeans model.
4. The results are saved into data.db file and visualized using **Grafana** dashboards to display the performance of the anomaly detection system.

## Troubleshooting

If you encounter any issues, check the following:

- Ensure that **Docker** and **Docker Compose** are properly installed.
- Make sure the **Grafana service** is running by checking the logs:

  ```bash
  docker compose logs grafana
  ```

- If the SQLite database cannot be accessed, verify the container is running:

  ```bash
  docker ps
  ```

- Ensure that the correct port (`3000`) is open for Grafana.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Feel free to reach out via GitHub issues if you have any questions or encounter any problems!
