# Import libraries
import random
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from cassandra.cluster import Cluster
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from airflow import DAG
from airflow.operators.python import PythonOperator

# Set up variables for ipaddresses
kafka_ip = "newkafka:9092"
topic = 'sensor_data_stream'  # Create topic to send data to
partitions = 3
replication_factor = 1
cassandra_ip = "172.20.0.6"

# Define default arguments
default_args = {"owner": "me",
                "retries": 1,
                "retry_delay": timedelta(minutes=1),
                "start_date": datetime.now()}

dag = DAG("test_streaming2",
          default_args=default_args,
          schedule=timedelta(minutes=2))


def sensor_streaming_data(ti, machines):
    """
    This function returns sensor data for a number of specified machines every 5 minutes.
    :param machines: determines the number of machines for which data is generated.
    :return:
    """
    try:
        machine_data = []
        for index in range(1, machines + 1):
            equipment_id = f"{index:03}"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            voltage = random.randint(200, 480)  # Measured in volts
            current = random.randint(0, 1000)  # Measured in Amps
            frequency = random.uniform(50, 60)  # Measured in hertz
            power_output = random.randint(10000, 10000000)  # Measured in watts
            fuel_consumption = random.randint(0, 500)  # Measured in Litre/hour
            engine_speed = random.randint(1000, 3000)  # Measured in RPM
            oil_pressure = random.randint(20, 80)  # Measured in psi
            coolant_temperature = random.uniform(60, 100)  # Measured in celcius
            exhaust_gas_temperature = random.uniform(200, 600)  # Measured in celcius
            battery_voltage = random.randint(12, 48)  # Measured in volts
            generator_load = random.randint(0, 100)  # Measured in %
            engine_hours = random.randint(5, 1000)  # Measured in hours
            vibration = random.randint(0, 500)  # Measured in mm/s
            coolant_level = random.randint(0, 10)  # Measured in liters
            fuel_level = random.randint(0, 50)  # Measured in liters

            sensor_data = {"equipment_id": equipment_id,
                           "timestamp": timestamp,
                           "voltage": voltage,
                           "current": current,
                           "frequency": frequency,
                           "power_output": power_output,
                           "fuel_consumption": fuel_consumption,
                           "engine_speed": engine_speed,
                           "oil_pressure": oil_pressure,
                           "coolant_temperature": coolant_temperature,
                           "exhaust_gas_temperature": exhaust_gas_temperature,
                           "battery_voltage": battery_voltage,
                           "generator_load": generator_load,
                           "engine_hours": engine_hours,
                           "vibration": vibration,
                           "coolant_level": coolant_level,
                           "fuel_level": fuel_level}
            machine_data.append(sensor_data)
        ti.xcom_push(key="sensor_data", value=machine_data)
    except Exception as e:
        print(f"Error: {e}")


# Define task for data generation
data_generation = PythonOperator(task_id="data_generation",
                                 python_callable=sensor_streaming_data,
                                 op_kwargs={"machines": 20},
                                 provide_context=True,
                                 dag=dag)


def kafka_producer(**kwargs):
    """
    This function creates a producer that collects data generated from sensors and sends them to kafka topics.
    :param kwargs: the data that is pulled from the sensor
    :return:
    """
    try:
        machine_data = kwargs['ti'].xcom_pull(task_ids="data_generation", key="sensor_data")  # Pull data from generator
        admin_client = AdminClient({'bootstrap.servers': kafka_ip})
        new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)
        admin_client.create_topics([new_topic])
        producer = Producer({'bootstrap.servers': kafka_ip})  # Create kafka producer

        def report_message(err, msg):
            if err is not None:
                print(f"Error: {err}")
            else:
                print(f"Message sent to broker: {msg.topic()}")

        for data_entry in machine_data:
            message_value = json.dumps(data_entry)
            print(message_value)
            message_value_bytes = message_value.encode('utf-8')
            producer.produce(topic, value=message_value_bytes, callback=report_message)
        producer.flush()
    except Exception as e:
        print(f"Error: {e}")


# Define kafka producer task
producer_task = PythonOperator(task_id="producer_task",
                               python_callable=kafka_producer,
                               dag=dag)


def kafka_consumer(ti):
    """
    This function creates a consumer that reads topics and sends the information to a dataframe to be processed
    :return:
    """
    try:
        max_iteration = 80
        iterations = 0
        raw_data = []
        consumer = Consumer({'bootstrap.servers': kafka_ip, 'group.id': 'sensor_data_consumer',
                             'auto.offset.reset': 'earliest'})  # Create consumer
        consumer.subscribe([topic])  # Subscribe to topic
        while iterations < max_iteration:
            final_message = consumer.poll(timeout=100.0)  # Poll for messages with a timeout
            if final_message is None:
                break
            else:
                if final_message.error():
                    print(f"Consumer error: {final_message.error()}")
                else:
                    rows = final_message.value().decode('utf-8')
                    sensor_data = json.loads(rows)
                    raw_data.append(sensor_data)
            iterations += 1
        ti.xcom_push(key='consumer_data', value=raw_data)
    except Exception as e:
        print(f"Error: {e}")


# Define kafka consumer task
consumer_task = PythonOperator(task_id="consumer_task",
                               python_callable=kafka_consumer,
                               provide_context=True,
                               dag=dag)


def transform_func(**kwargs):
    """
    This function collects data from kafka consumers and converts that json to a dataframe before transforming it.
    It also sends the transformed data using xcom push to the next task.
    :param kwargs: data pulled from consumer task.
    :return:
    """
    try:
        jsonlist = kwargs['ti'].xcom_pull(task_ids='consumer_task', key='consumer_data')
        df = pd.DataFrame(jsonlist)
        check = pd.DataFrame(jsonlist)
        df['coolant_temperature'] = df['coolant_temperature'].round(2)  # Perform some transformations
        df['frequency'] = df['frequency'].round(2)
        df['exhaust_gas_temperature'] = df['exhaust_gas_temperature'].round(2)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%d %H:%M:%S")

        cluster = Cluster([cassandra_ip])  # Connect to database to retrieve maximum timestamp
        conn = cluster.connect()
        conn.execute(
            "CREATE KEYSPACE IF NOT EXISTS generator_sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1}")
        conn.set_keyspace("generator_sensor")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sensor_data (equipment_id VARCHAR, timestamp TIMESTAMP, voltage INT,  current INT, frequency DECIMAL, power_output INT, fuel_consumption INT,  engine_speed INT, oil_pressure INT, coolant_temperature DECIMAL, exhaust_gas_temperature DECIMAL, battery_voltage INT,generator_load INT,  engine_hours INT,  vibration INT,  coolant_level INT,  fuel_level INT, PRIMARY KEY ((equipment_id), timestamp)  )")
        result = conn.execute("SELECT MAX(timestamp) FROM sensor_data").one()
        max_time_db = pd.to_datetime(result[0], format="%Y-%m-%d %H:%M:%S")
        print(f"The current maximum time in db is {max_time_db}")
        if max_time_db is None:
            print("First dataset received from topic")
        else:
            df = df[df['timestamp'] > max_time_db]
            print(f"Reduced total dataset {len(check)} to recent streamed data {len(df)}")
        kwargs['ti'].xcom_push(key='transformed_data', value=df)
    except Exception as e:
        print(f"Error: {e}")


# Define transform task
transform_task = PythonOperator(task_id="transform_task",
                                python_callable=transform_func,
                                dag=dag)


def load_func(**kwargs):
    """
    This function collects transformed sensor data and loads it into a table in Cassandra DB
    :param kwargs: transformed data from transform_task
    :return:
    """
    try:
        data = kwargs['ti'].xcom_pull(task_ids='transform_task', key="transformed_data")
        cluster = Cluster([cassandra_ip])
        conn = cluster.connect()
        conn.set_keyspace("generator_sensor")
        columns = ", ".join(data.columns)
        placeholders = ", ".join(['%s'] * len(data.columns))
        query = f"INSERT INTO sensor_data({columns}) VALUES ({placeholders})"
        for index, row in data.iterrows():
            # Convert timestamp to string in ISO 8601 format
            row['timestamp'] = row['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(query, tuple(row))
    except Exception as e:
        print(f"Error: {e}")


# Define load task
load_task = PythonOperator(task_id="load_task",
                           python_callable=load_func,
                           dag=dag)

# Define DAG dependencies
data_generation >> producer_task >> consumer_task >> transform_task >> load_task


