    In this project we plan to implement Machine Learning with Spark MLlib to detect spam emails
# Spark MLlib Spam Email Classifier

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.0%2B-orange.svg)](https://spark.apache.org/)

**Detect spam emails using Apache Spark's MLlib.**

This project leverages Apache Spark's MLlib to implement a machine learning model capable of classifying emails as spam or not spam.

## ✨ Features

- **Distributed Processing**: Utilizes Apache Spark for handling large datasets efficiently.
- **Machine Learning**: Implements classification algorithms using Spark MLlib.
- **Scalability**: Designed to scale with increasing data volumes.

## 📁 Project Structure

```
spark-bd-project/
├── src/                 # Source code for data processing and model training
├── BD_442_458_557_585.pdf  # Project report/documentation
├── .gitignore           # Git ignore file
└── README.md            # Project documentation
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Apache Spark 3.0 or higher
- Java 8 or higher
- pip (Python package installer)

### Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/ShattereDarkness/spark-bd-project.git
   cd spark-bd-project
   ```

2. **Set Up Apache Spark**

   - Download and install Apache Spark from the [official website](https://spark.apache.org/downloads.html).
   - Set the `SPARK_HOME` environment variable to the Spark installation directory.
   - Add `$SPARK_HOME/bin` to your system's `PATH`.

3. **Create a Virtual Environment (Optional but Recommended)**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

4. **Install Python Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

   *Note: Ensure that `requirements.txt` exists in the project root directory with all necessary dependencies listed.*

## 📝 Usage

1. **Prepare the Dataset**

   - Place your email dataset in the `data/` directory.
   - Ensure the dataset is in a format compatible with the processing scripts (e.g., CSV with appropriate columns).

2. **Run the Application**

   ```bash
   spark-submit src/main.py --input data/emails.csv
   ```

   Replace `data/emails.csv` with the path to your dataset.

3. **Output**

   - The model's predictions will be saved in the `output/` directory.
   - Evaluation metrics will be displayed in the console and saved to `output/metrics.txt`.

## 🧪 Testing

To run the test pipeline:

```bash
python -m unittest discover tests
```

This will execute all unit tests located in the `tests/` directory.
