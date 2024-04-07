# Recipe Data Processing with PySpark

This project processes recipe data using PySpark, extracting recipes containing variations of the word "chilies," calculating the difficulty level of each recipe based on cooking and preparation times, and aggregating total minutes per difficulty level.

## Prerequisites

Before running the code, ensure you have the following installed on your system:

- [Python 3.x](https://www.python.org/)
- [Apache Spark](https://spark.apache.org/downloads.html)
- NLTK library for Python (for stemming)

## Installation and Setup

1. **Install Python 3.x**: If you don't have Python installed, download and install it from the [official Python website](https://www.python.org/).

2. **Install Apache Spark**:
   
   - Download Apache Spark from the [official website](https://spark.apache.org/downloads.html).
   - Extract the downloaded archive to your preferred location.
   - Set up Spark environment variables:

     ```bash
     export SPARK_HOME=/path/to/your/spark/directory
     export PATH=$PATH:$SPARK_HOME/bin
     ```

3. **Create Python venv**:
   python3 -m venv venv
   source venv/bin/activate (For MAC)
   venv\Scripts\activate (Windows)


4. **Install dependencies/Library**:

   Install NLTK, Lavenshtein, requests, PySpark using pip in venv

   ```bash
   pip install -r requirements.txt

