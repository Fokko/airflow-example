footer: © GoDataDriven, Fokko Driesprong(@godatadriven.com), @fokkodriesprong
slidenumbers: true

# Airflow Code Breakfast

![inline](godata.png)

---

![](uni.png)

# Whoami

## Fokko Driesprong

- Master Distributed Systems & Software Engineering
- Works at GoDataDriven as Data Engineer
- Apache PMC Member and Apache Airflow Committer
- Contributer to Apache Flink, Apache Spark and Druid

---

![](oil.jpg)

- Complex network of dependancies
- Analytics and batch processing is mission critical
- Too much time is spend on monitoring and troubleshooting jobs

---

![](airflow.png)

# Apache Airflow

- Programatically workflow scheduling

the sequence of industrial, administrative, or other processes through which a piece of work passes from initiation to completion.


- Developed by Airbnb in 2015
- We love open source
- {ETL, Machine Learning, Predictive, General} pipeline
- Used by 120+ companies, among Airbnb, ING, LinkedIn, Paypal, WePay, HBO and more
- ~350 contributors

---

![](airflow.png)

# Apache Airflow

- Monitors the processes
- Logs runtime
- Handles connections and credentials
- Retries (and notification) on failures
- Resource pooling

---

# Data pipeline dependancies

- ETL from an external system
- Feature extraction
- Training models
- Shipping it to production

---

# What is important?

- Scalability
- Availability
- Alert-enabled
- Error recovery
- Correctness

---

# Airflow's primives

- Directed Acyclic Graph (DAG)
- Operators
- Sensors
- Hooks

---

# DAG

- Defines a workflow in Python
- Scheduled periodically
- Encapsulates dependancies

---

# Operator

	- Bash
	- Docker
	- Hive
	- HTTP
	- MySQL/MsSQL/Oracle/Postgres
	- Redshift/S3
	- Slack/Email
	- Python

---

# Operator (Community!)

	- Bigquery/Dataflow/Mlengine/Pubsub
	- Qubole
	- Spark
	- EMR
	- Druid
	- Sqoop
	- SSH
	- many more...

---

# Hooks

- Connections
- HiveToDruid operator
	- HiveHook
	- DruidHook

---

# Sensors

- Senses external state
	- HdfsSensor, look for _SUCCESS

---

# Lets play around

---

# Deployment

- On premise
	- Docker
	- Ansible (https://github.com/godatadriven/airflow-ansible)
- In the cloud
	- Postgres as a service
	- SNS/SQS for Celery

---

# Executor

- Sequential
- Local
- Celery
- Dask
- Docker underway

---

# Thanks for the attention!

---

# Any questions?
