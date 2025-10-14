# 🚖 Ride Sharing Analytics Using Spark Streaming and Spark SQL

This project demonstrates a **real-time data analytics pipeline** for a ride-sharing platform using **Apache Spark Structured Streaming** and **Spark SQL**.  
It processes live trip data, performs driver-level aggregations, and analyzes earnings trends over time.

---

## 🧩 Overview

The system continuously ingests JSON ride data from a socket stream and performs:
- **Task 1:** Real-time data ingestion and parsing  
- **Task 2:** Real-time aggregations (driver-level earnings & distances)  
- **Task 3:** Time-based window analytics on fare amounts  

Each task outputs structured CSV files in the respective folder under `outputs/`.

---

## ⚙️ Prerequisites

Before running the project, ensure the following software is installed and properly configured.

### 🐍 Python 3.x
- [Download Python](https://www.python.org/downloads/)
- Verify installation:
  ```bash
  python3 --version


## 🔥 PySpark

Install PySpark using pip:
```bash
pip install pyspark
```
## 🧑‍💻 Faker

Install Faker for data generation:
```bash
pip install faker
```

## 📁 Project Structure

```bash
hands_on_9/
├── outputs/
│   ├── task1/
│   │   └── CSV files of task 1
│   ├── task2/
│   │   └── CSV files of task 2
│   └── task3/
│       └── CSV files of task 3
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

### 🗂️ Descriptions

**- data_generator.py** – Generates continuous streaming data with schema
```bash
(trip_id, driver_id, distance_km, fare_amount, timestamp)
```

**- task1.py** – Reads and parses real-time JSON data stream

**- task2.py** – Performs live aggregations per driver (total fare, average distance)

**- task3.py** – Analyzes fare trends using time-based windowed aggregation

**- outputs/** – Stores all task results in CSV format


## 🚀 Setup and Execution
**🧩 Step 1 – Start Data Generator**
Run the data generator to simulate live trip data:

```bash
python data_generator.py
```
⚠️ Keep this terminal running continuously — it streams JSON data to the socket.

**▶️ Step 2 – Run Each Task in a New Terminal**
**🟩 Task 1 – Streaming Ingestion & Parsing**
```bash
python task1.py
```

**What it does:**

Reads live JSON data from socket (localhost:9999)

**Parses into structured columns:**
```bash
trip_id, driver_id, distance_km, fare_amount, timestamp
```

**Writes parsed data to:**
```bash
outputs/task1/
```

**🟦 Task 2 – Real-Time Aggregations**
```bash
python task2.py
```

**What it does:**

Aggregates data in real-time by driver_id

**Calculates:**

SUM(fare_amount) as total_fare

AVG(distance_km) as avg_distance

Writes results continuously to:
```bash
outputs/task2/
```

**🟨 Task 3 – Windowed Time-Based Analytics**
```bash
python task3.py
```

**What it does:**

Converts string timestamp to Spark TimestampType

Performs a 5-minute window aggregation sliding every 1 minute

Adds 1-minute watermark to handle late data

**Aggregates fare amounts:**

SUM(fare_amount) as total_fare_per_window

Writes results to:
```bash
outputs/task3/
```
## 📂 Verify Outputs

---

You can check all generated output files using:
```bash
ls outputs/
ls outputs/task1/
ls outputs/task2/
ls outputs/task3/
```

Each folder contains CSV files of processed results for the corresponding task.

---

### 🧠 Learning Outcomes

Through this project, I successfully:

- Built a **real-time data ingestion pipeline** using *Apache Spark Structured Streaming* to process continuous JSON ride data.
- Implemented **driver-level real-time analytics** to calculate total fares and average distances using *Spark SQL* aggregations.
- Developed **time-based window analytics** to monitor trends in fare amounts with 5-minute sliding windows and watermarking.
- Managed **streaming checkpoints**, **output sinks**, and handled real-time data flow efficiently across multiple Spark jobs.
- Gained hands-on experience in **stream processing**, **data engineering**, and **real-time analytics workflows** using PySpark.

---

### 💡 Key Takeaways

- Learned to work with **socket streams** and simulate continuous input data with *Faker*.
- Improved debugging and troubleshooting skills for streaming applications.
- Understood the difference between **micro-batching** and **continuous processing** in Spark.
- Applied data pipeline concepts that mirror **real-world streaming analytics systems** used by ride-sharing platforms like Uber or Lyft.
- Strengthened overall proficiency in **PySpark**, **data streaming**, and **real-time data analytics**.

---

✅ *This project demonstrates my ability to build end-to-end Spark Streaming pipelines, perform live analytics, and handle time-based data aggregations effectively.*
