Apache Kafka and ZooKeeper in Windows
Introduction
Apache Kafka is a distributed platform for real-time data streaming and processing whereas ZooKeeper is a centralized service for configuration management and synchronization in distributed systems.

ZooKeeper Installation
Download ZooKeeper from the official releases page to C Directory.
Go to your ZooKeeper config directory. For example: C:\zookeeper-3.9.2\conf
Rename the file zoo_sample.cfg to zoo.cfg
Open zoo.cfg in any text editor, and edit the line:
dataDir=/tmp/zookeeper
to:
dataDir=C:/zookeeper-3.9.2/data
Kafka Installation
Download Kafka from the official downloads page.

Go to your Kafka config directory. For example: C:\kafka_2.13-3.7.0\config

Find the file server.properties and edit the line:

log.dirs=/tmp/kafka-logs
to:

log.dirs=C:/kafka_2.13-3.7.0/kafka-logs
If your ZooKeeper is running on a different machine or cluster, edit the line:

zookeeper.connect=localhost:2181
to your custom IP and port.

Note: Your Kafka will run on the default port 9092 and connect to ZooKeeper’s default port 2181.

Environment Variables and Path Configuration
User Variables
Add the following user variable:

ZOOKEEPER_HOME = C:\zookeeper-3.9.2
Path
Add the following paths to the system Path variable:

%ZOOKEEPER_HOME%\bin
C:\kafka_2.13-3.7.0\bin\windows
Running ZooKeeper
Open a command prompt and enter:

zkserver
Running a Kafka Server
Important: Ensure that your ZooKeeper instance is up and running before starting the Kafka server.

Run the following command in a command prompt:

kafka-server-start.bat C:\kafka_2.13-3.7.0\config\server.properties
Dependencies
Install the necessary dependencies:

pip install -r requirements.txt