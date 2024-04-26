# Batch Streaming ETL for Sensor Data

## Table of Contents
1. [Project Overview](#project-overview)
2. [Data Generation](#data-generation)
3. [Kafka Streaming](#kafka-streaming)
4. [Data Transformation](#data-transformation)
5. [Data Loading](#data-loading)
6. [DAG Definition](#dag-definition)

### Project Overview:
This project focuses on implementing a batch streaming ETL pipeline for processing sensor data collected from various machines. The data includes sensor readings such as voltage, current, frequency, temperature, and more. The pipeline extracts sensor data, processes it in real-time, transforms it, and loads it into a Cassandra database for further analysis.

![Screenshot](https://imgur.com/a/YRB7Tas)

### Data Generation:
The sensor data is generated synthetically for a specified number of machines. Each machine generates sensor readings at regular intervals, simulating real-world scenarios. The generated data includes information such as equipment ID, timestamp, voltage, current, frequency, and other sensor readings.

### Kafka Streaming:
Apache Kafka is used as the messaging system for streaming sensor data. A Kafka producer collects the generated sensor data and sends it to Kafka topics for further processing. A Kafka consumer reads the data from the topics and passes it to the ETL pipeline for transformation.

### Data Transformation:
The received sensor data is transformed using Pandas DataFrame operations. Timestamps are converted to the appropriate format, and certain sensor readings are rounded to ensure consistency and accuracy. Additionally, the pipeline compares the timestamps of incoming data with the maximum timestamp stored in the Cassandra database to ensure that only new data is processed.

### Data Loading:
Transformed sensor data is loaded into a Cassandra database table named `sensor_data`. The pipeline establishes a connection to the Cassandra cluster, prepares the data for insertion, and executes the insertion query to load the data into the database. Each row of data corresponds to a specific timestamp and equipment ID.

### DAG Definition:
The Directed Acyclic Graph (DAG) is defined using Apache Airflow, orchestrating the entire ETL process. It consists of the following tasks:
- **Data Generation**: Generates synthetic sensor data for a specified number of machines.
- **Kafka Producer**: Collects generated sensor data and sends it to Kafka topics.
- **Kafka Consumer**: Reads sensor data from Kafka topics for processing.
- **Data Transformation**: Transforms the received sensor data using Pandas DataFrame operations.
- **Data Loading**: Loads transformed sensor data into the Cassandra database.

The DAG ensures the sequential execution of tasks, allowing for efficient data processing and loading.

By following this batch streaming ETL pipeline, sensor data from multiple machines can be effectively processed and analyzed for insights into equipment performance and operational efficiency.
