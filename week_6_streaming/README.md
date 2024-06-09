# Table of contents

- [DE Zoomcamp 6.1-Introductio](#de-zoomcamp-61-introduction)
- [DE Zoomcamp 6.2-What is stream processing](#de-zoomcamp-62-what-is-stream-processing)
  - [Data exchange](#data-exchange)
  - [Stream processing](#stream-processing)
- [DE Zoomcamp 6.3-What is kafka?](#de-zoomcamp-63-what-is-kafka)
  - [Basic Kafka components - Topic](#basic-kafka-components---topic)
  - [Basic Kafka components - Events](#basic-kafka-components---events)
  - [Basic Kafka components - Logs](#basic-kafka-components---logs)
  - [Basic Kafka components - Message](#basic-kafka-components---message)
  - [Basic Kafka components - Broker & Cluster](#basic-kafka-components---broker--cluster)
  - [Intermission: visualizing the concepts so far](#intermission-visualizing-the-concepts-so-far)
  - [consumer_offsets](#consumer_offsets)
  - [Consumer Groups](#consumer-groups)
  - [Partitions](#partitions)
  - [Replication](#replication)
  - [What makes Kafka special?](#what-makes-kafka-special)
  - [Stream processing in real-world scenarios](#stream-processing-in-real-world-scenarios)
- [DE Zoomcamp 6.4-Confluent cloud](#de-zoomcamp-64-confluent-cloud)
  - [Using Docker](#using-docker)
  - [Using Confluent](#using-confluent)
    - [Create cluster](#create-cluster)
    - [Create API Key](#create-api-key)
    - [Create topic](#create-topic)
    - [Produce a message](#produce-a-message)
    - [Connectors](#connectors)
- [DE Zoomcamp 6.13-Kafka Streaming with Python](#de-zoomcamp-613-kafka-streaming-with-python)
  - [Json example](#json-example)
  - [Avro example](#arvo-example)

# [DE Zoomcamp 6.1-Introduction](https://www.youtube.com/watch?v=hfvju3iOIP0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=67)

- What is stream processing?
- What is Kafka?
- How does Kafka plays a role in stream processing?
- Some of the message properties of stream processing.
- Configuration parameters specific to Kafka.
- Time surrounding in stream processing.
- Kafka producers & Kafka consumers.
- How actually data is partitioned inside stream processing.
- Example of how to work with Kafka stream (Java).
- Spark streaming python examples.
- Schema and it's roles in stream processing.

# [DE Zoomcamp 6.2-What is stream processing](https://www.youtube.com/watch?v=WxTxKGcfA-k&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=68)

In this lesson, we will discuss what `stream processing` is. But before we dive into that, let's first talk about `data exchange`.

### Data exchange

`Data exchange` can occur through various channels. In the real world, a common form of `data exchange` is when a producer posts a flyer on a notice board to share information with the public. On the other hand, consumers or users passing by can read, react, or take necessary actions based on the information provided, or simply ignore it if it's not relevant to them.

![data-exchange-example](./images/data-exchange-example.png)

Regarding computer communication, we often refer to APIs such as REST, GraphQL, and webhooks. The concept remains the same - one computer shares information or data, which is then exchanged through these services. Imagine you are a consumer interested in specific topics like Kafka, Spark stream processing, and Big Data. As a producer, I can attach my flyer to a particular topic.

![data-exchange-example-kafka](./images/data-exchange-example-kafka.png)

### Stream processing

Unlike batch mode, data is exchanged more dynamically in stream processing. The data is exchanged in `real time, without any delays`. In this example, the producer sends data to a Kafka topic. The Kafka topic receives the message instantly and delivers it to the consumer right away.

To clarify what we mean by `real time`. It doesn't mean instantaneously like the speed of light. There might be a few seconds of delay. Nevertheless, it is much faster than before. In `batch processing`, data is consumed every minute or every hour, but in real time (`stream processing`), the messages come in much faster.

![stream-processing-example](./images/stream-processing-example.png)

# [DE Zoomcamp 6.3-What is kafka?](https://www.youtube.com/watch?v=zPLZUDPi4AY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=69)

We will explore Kafka streaming and how we can use it as a central streaming architecture for our similar notice board example. We will delve into the details of Kafka, including its scalability, robustness, and configuration.

Kafka is used to upgrade from a project architecture like this...
![before-using-kafka](./images/before-using-kafka.png)

...to an architecture like this:
![after-using-kafka](./images/after-using-kafka.png)

`Apache Kafka` is a **_message broker_** and **_stream processor_**. Kafka is used to handle `real-time data` feeds. Kafka works by allowing producers to send messages which are then pushed in real time by Kafka to consumers. Kafka is hugely popular and most technology-related companies use it.

You can also check out this [animated comic](https://www.gentlydownthe.stream/) to learn more about Kafka.

- In the example of notice board, we have two topics: ABC topic and XYZ topic.
  - `Producers` who produce data to these topics, which can be relevant to specific use cases or scenarios.
  - `Consumers` are those that consume the data from these topics: web pages, micro services, apps, etc.

Connecting consumers to producers directly can lead to an amorphous and hard to maintain architecture in complex projects like the one in the first image. Kafka solves this issue by becoming an intermediary that all other components connect to.

![kafka-topics](./images/kafka-topics.png)

### Basic Kafka components - Topic

A topic is an abstraction of a concept. Concepts can be anything that makes sense in the context of the project, such as "sales data", "new members", "clicks on banner", etc. A producer pushes a message to a topic, which is then consumed by a consumer subscribed to that topic.

Kafka uses the term `topic` extensively, What is `topic`? It's a continuous stream of `events`.

![what-is-inside-topic](./images/what-is-inside-topic.png)

### Basic Kafka components - Events

And what is `events`? These `events`, over time, are simply data points at a specific timestamp. The collection of these `events` goes into our `topic`, and our consumer in Kafka reads these `events`.

### Basic Kafka components - Logs

Kafka stores data in the form of logs, which is the way events are stored in a topic. When discussing logs, we are essentially discussing how data is stored within the topic. Logs store messages in an ordered fashion. Kafka assigns a sequence ID in order to each new message and then stores it in logs.

### Basic Kafka components - Message

The basic communication abstraction used by producers and consumers in order to share information in Kafka is called a `message`. It consists of three main components:

- `Key`: used to identify the message and for additional Kafka stuff such as partitions (covered later).
- `Value`: the actual information that producers push and consumers are interested in.
- `Timestamp`: indicates the exact time when the message was created, used for logging.

![kafka-message](./images/kafka-message.png)

### Basic Kafka components - Broker & Cluster

A `Kafka broker` is a machine (physical or virtualized) on which Kafka is running.

A `Kafka cluster` is a collection of brokers (nodes) working together.

### Intermission: visualizing the concepts so far

Here's how a producer and a consumer would talk to the same Kafka broker to send and receive messages.

- Producer sending messages to Kafka.

  ![intermission-visualizing-concept-producer](./images/intermission-visualizing-concept-producer.png)

  1. The producer first declares the topic it wants to "talk about" to Kafka. In this example, the topic will be `abc`. Kafka will then assign a _physical location_ on the hard drive for that specific topic (the topic logs).
  2. The producer then sends messages to Kafka (in our example, messages 1, 2 and 3).
  3. Kafka assigns an ID to the messages and writes them to the logs.
  4. Kafka sends an acknowledgement to the producer, informing it that the messages were successfully sent and written.

- Consumer receiving messages

  Broker and logs are the same as those in the first graph; the graph has been split in 2 for clarity.

  ![intermission-visualizing-concept-consumer](./images/intermission-visualizing-concept-consumer.png)

  1. The consumer declares to Kafka that it wants to read from a particular topic. In our example, the topic is `abc`.
  2. Kafka checks the logs and figures out which messages for that topic have been read and which ones are unread.
  3. Kafka sends the unread messages to the consumer.
  4. The consumer sends an acknowledgement to Kafka, informint it that the messages were successfully received.

### consumer_offsets

The workflows work fine for a single consumer but it omits how it keeps track of read messages. It also doesn't show what would happen if 2 or more consumers are consuming messages for the same topic.

**_`__consumer_offsets`_** is a special topic that keeps track of messages read by each consumer and topic. In other words: _Kafka uses itself_ to keep track of what consumers do.

When a consumer reads messages and Kafka receives the ack, Kafka posts a message to `__consumer_offsets` with the consumer ID, the topic and the message IDs that the consumer has read. If the consumer dies and spawns again, Kafka will know the last message delivered to it in order to resume sending new ones. If multiple consumers are present, Kafka knows which consumers have read which messages, so a message that has been read by consumer #1 but not by #2 can still be sent to #2.

### Consumer Groups

A `consumer group` is composed of multiple consumers.

In regards of controlling read messages, Kafka treats all the consumers inside a consumer group as a _single entity_: when a consumer inside a group reads a message, that message will **_NOT_** be delivered to any other consumer in the group.

Consumer groups allow consumer apps to scale independently: a consumer app made of multiple consumer nodes will not have to deal with duplicated or redundant messages.

Consumer groups have IDs and all consumers within a group have IDs as well.

The default value for consumer groups is 1. All consumers belong to a consumer group.

### Partitions

> Note: do not confuse BigQuery or Spark partitions with Kafka partitions; they are different concepts.

Topic logs in Kafka can be **_partitioned_**. A topic is essentially a _wrapper_ around at least 1 partition.

Partitions are assigned to consumers inside consumer groups:

- **_A partition_** is assigned to **_one consumer only_**.
- **_One consumer_** may have **_multiple partitions_** assigned to it.
- If a consumer dies, the partition is reassigned to another consumer.
- Ideally there should be as many partitions as consumers in the consumer group.
  - If there are more partitions than consumers, some consumers will receive messages from multiple partitions.
  - If there are more consumers than partitions, the extra consumers will be idle with nothing to do.

Partitions in Kafka, along with consumer groups, are a scalability feature. Increasing the amount of partitions allows the consumer group to increase the amount of consumers in order to read messages at a faster rate. Partitions for one topic may be stored in separate Kafka brokers in our cluster as well.

Messages are assigned to partitions inside a topic by means of their **_key_**: message keys are hashed and the hashes are then divided by the amount of partitions for that topic; the remainder of the division is determined to assign it to its partition. In other words: _hash modulo partition amount_.

- While it would be possible to store messages in different partitions in a round-robin way, this method would not keep track of the _message order_.
- Using keys for assigning messages to partitions has the risk of making some partitions bigger than others. For example, if the topic `client` makes use of client IDs as message keys and one client is much more active than the others, then the partition assigned to that client will grow more than the others. In practice however this is not an issue and the advantages outweight the cons.

![kafka-partitions](./images/kafka-partitions.png)

### Replication

Partitions are **_replicated_** accross multiple brokers in the Kafka cluster as a fault tolerance precaution.

When a partition is replicated accross multiple brokers, one of the brokers becomes the **_leader_** for that specific partition. The leader handles the message and writes it to its partition log. The partition log is then replicated to other brokers, which contain **_replicas_** for that partition. Replica partitions should contain the same messages as leader partitions.

If a broker which contains a leader partition dies, another broker becomes the leader and picks up where the dead broker left off, thus guaranteeing that both producers and consumers can keep posting and reading messages.

We can define the _replication factor_ of partitions at topic level. A replication factor of 1 (no replicas) is undesirable, because if the leader broker dies, then the partition becomes unavailable to the whole system, which could be catastrophic in certain applications.

### What makes Kafka special?

In terms of data exchange, Kafka provides `robustness`, `flexibility`, and `scalability` as its core features.

Kafka provides a feature called `replication`, which allows Kafka to replicate data across different fields. This ensures the `robustness` and `reliability` needed for building a stream processing system. Event if your servers or nodes go down, you will still receive the data.

It also provides `flexibility`, allowing topics to be of any size and accommodating multiple consumers. Kafka allows you to store data cost-effectively while still having access to it for offline analysis or retrieval into the topic. This allows some messages to be stored for a longer period of time. Just because one consumer has read the message doesn't mean that other consumers cannot read it anymore

Kafka is highly `scalable`, meaning it can handle an increase in data size from 10 events per second to 1000 events per second without any issues. This is one of the main reasons why Kafka is so popular in the streaming space.

![kafka-features](./images/kafka-features.png)

### Stream processing in real-world scenarios

Not too long ago, our architecture consisted of `monoliths` that primarily interacted with a central database. These `monoliths` were typically large pieces of code that could communicate with one or multiple databases.

However, this architecture started causing issues. Without going into too much detail about `monoliths` versus `microservices`, the current trend is to move towards `microservices`. In this case, we now have many small applications that together form the microservice architecture.

These `microservices` still need to communicate with each other. They can do so through APIs, message passing, or by accessing a central database that is accessible to all of them. Sometimes, `microservices` may have access to specific databases while others do not. This architecture works well as long as the microservices and data size are not too large.

![kafka-microservices](./images/kafka-mircoservices.png)

However, as the number of `microservices` and data increases, it becomes necessary to have a consistent message passing or streaming service. In this case, one microservice typically writes to a Kafka topic, which represents events, and other trusted `microservices` can consume these events.

Sometimes, it's required to have multiple data sources, so these `microservices` work closely with the database or the monolith service. In this case, we will have Kafka messages, and the database would be able to write to the Kafka topic. Any microservice interested can read from these Kafka topics, and the process is called `CDC (Change Data Capture)`.

![kafka-cdc](./images/kafka-cdc.png)

# [DE Zoomcamp 6.4-Confluent cloud](https://www.youtube.com/watch?v=ZnEZFEYKppw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=70)

Install instructions for Kafka can be found in [the official website](https://kafka.apache.org/quickstart#quickstart_kafkaconnect).

### Using Docker

Due to the complexity of managing a manual Kafka install, a docker-compose script is provided [in this link](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/6_streaming/docker-compose.yml). The Docker images are provided by [Confluent](https://www.confluent.io/), a Kafka tool vendor. The script defines the following services:

- **[`zookeeper`](https://zookeeper.apache.org/)**: a centralized service for maintaining configuration info. Kafka uses it for maintaining metadata knowledge such as topic partitions, etc.
  - Zookeeper is being phased out as a dependency, but for easier deployment we will use it in the lesson.
- **`broker`**: the main service. A plethora of environment variables are provided for easier configuration.
  - The image for this service packages both Kafka and [Confluent Server](https://docs.confluent.io/platform/current/installation/migrate-confluent-server.html), a set of commercial components for Kafka.
- **`kafka-tools`**: a set of additional Kafka tools provided by [Confluent Community](https://www.confluent.io/community/#:~:text=Confluent%20proudly%20supports%20the%20community,Kafka%C2%AE%EF%B8%8F%2C%20and%20its%20ecosystems.). We will make use of this service later in the lesson.
- **`schema-registry`**: provides a serving layer for metadata. We will make use of this service later in the lesson.
- **`control-center`**: a web-based Kafka GUI.
  - Kafka can be entirely used with command-line tools, but the GUI helps us visualize things.

Download the script to your work directory and start the deployment with `docker-compose up` . It may take several minutes to deploy on the first run. Check the status of the deployment with `docker ps` . Once the deployment is complete, access the control center GUI by browsing to `localhost:9021` .

### Using Confluent

we are gonna set up our Confluent Cloud free trial that basically allows you to have a Kafka cluster. Confluent provides 30 days free trial with 400$ and it can be easily connected to Cloud services such as GCP. Most of the things you will do in here would be free you would not need a credit card or debit card and it's an easy way to set this up.

#### Create cluster

Firstly, what we need to do is basically create a cluster. Let's use GCP at location Frankfurt.

![confluent-create-cluster-1](./images/confluent-create-cluster-1.png)

![confluent-create-cluster-2](./images/confluent-create-cluster-2.png)

Next step will be payment method, we can skip this step.

Then we are going to name our clusters as `kafka_tutorial_cluster`, and click "Launch cluster".

![confluent-create-cluster-3](./images/confluent-create-cluster-3.png)

This is the view when the cluster is successfully created.

![confluent-create-cluster-done](./images/confluent-create-cluster-done.png)

#### Create API Key

We need an API key to interact with the Confluent; therefore we are going to create an API key by following steps:

![confluent-create-api-key-1](./images/confluent-create-api-key-1.png)

![confluent-create-api-key-2](./images/confluent-create-api-key-2.png)

Enter the "Description" field: `kafka_tutorial_api_key`. Then select "Download and Continue". Save the API file on your local machine.

#### Create topic

Next step, we are going to create a topic named as `kafka_tutorial` with the `Partitions` is 2, `Retention time` is 1 day in `Advance setting`, because we don't want to pay a lot. Then click "Save and Create" button.

![confluent-create-topic-1](./images/confluent-create-topic-1.png)

![confluent-create-topic-2](./images/confluent-create-topic-2.png)

#### Produce a message

Next step we will create a message in Confluent by following these steps:

![confluent-produce-message-1](./images/confluent-produce-message-1.png)

![confluent-produce-message-2](./images/confluent-produce-message-2.png)

#### Connectors

Let's create a dummy `Connector`, in this case we are going to use `Sample Data`.

![confluent-connector-1](./images/confluent-connector-1.png)

![confluent-connector-2](./images/confluent-connector-2.png)

Generate the API key & download to your local machine.

![confluent-connector-3](./images/confluent-connector-3.png)

![confluent-connector-4](./images/confluent-connector-4.png)

![confluent-connector-5](./images/confluent-connector-5.png)

![confluent-connector-6](./images/confluent-connector-6.png)

After a few minutes, the `Connector` is successfully create. We can go to the `Topic` and check the `Message` that we are receving streaming message from the `Connector`.

![confluent-connector-streaming-message](./images/confluent-connector-streaming-message.png)

_**NOTE:**_ In order to save the credit, pause/turn off the `Connector` when not using.

![confluent-connector-pause](./images/confluent-connector-pause.png)

# [DE Zoomcamp 6.4-Kafka producer consumer](https://www.youtube.com/watch?v=aegTuyxX7Yg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=71)

In this course, we will also programmatically consume data using Java as the programming language. Kafka libraries are well maintained for Java, while Python is not as well maintained currently.

we will use the Confluent cloud. The first step is to create a topic for `rides data`. Click `Add topic` and configure as following:

![confluent-java-topic-1](./images/confluent-java-topic-1.png)

Next, we will connect it to a `client`. The `client` provides examples on how to do this, particularly in Java. We will follow the configuration instructions provided.

Download the source code for [Java example with Kafka](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-streaming/java/kafka_examples_) and then follow along the tutorials for writing the Java code.

# [DE Zoomcamp 6.13-Kafka Streaming with Python](https://www.youtube.com/watch?v=BgAlVknDFlQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=80)

- [Json Producer-Consumer Example](./code/python/json_example/) using kafka-python library
- [Avro Producer-Consumer Example](./code/python/avro_example) using confluent-kafka library

In this session, demonstrating how to implement producer-consumer functionality using Python libraries and using Docker (instead of Confluent).

Packages need to be installed:

```bash
pip3 install kafka-python confluent-kafka fastavro
```

Instead of setting up an extra Confluent cloud, we will be running Kafka-specific services in a Docker container. To get started, created a `Docker folder` which includes Kafka and Spark. For this tutorial, we only need Kafka and its services to be up and running.

Let's take a look at the services in the [docker-compose.yml](./code/python/docker/docker-compose.yml) file and the configurations required in our system. There are:

- `broker`: some parameters are important for us to understand such as: `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS`, `KAFKA_INTER_BROKER_LISTENER_NAME`, `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`. These parameters configure how kakfa communicate with the docker and how we can access broker outside of the docker. `PLAINTEXT` defines how Kafka communicate within inter-broker-listener using the port 29092 for inter-communication within the Kafka cluster. `PLAINTEXT_HOST` represents how we can access to the Kafka cluster from outside with port 9092. These parameters hold significant role and it is highly recommended to read and understand why we have two different definitions.
- `schema-registry`
- `zookeeper` -`control-center`
- `kafka-rest`

The first four services are necessary for running Kafka successfully, while `kafka-rest` is optional and allows us to communicate with the Kafka broker using the REST API. Is is useful when debugging messages or ensuring everything is functioning correctly, we may need to access it through the REST API. To get started, we just need to have the specific network configuration in place. Normally, we don't need to have a specific network for this.

However, let's focus on creating the network because this network is necessary for the upcoming tutorials where we will be working with Spark and Kafka.

```bash
# Create Network
docker network create kafka-spark-network

# List all Networks
docker network ls

# Run docker container
docker-compose up -d

# Run producer.py
python3 producer.py

# Run consumer.py
python3 consumer.py
```

If you have not, this is the only package you need to install in your virtual environment for this Redpanda lesson.

### Json example

Let's delve into the `json_example`. The objective is to read a CSV file from our local machine, specifically the `rides.csv` file located in the `resources folder`.

We will create a `producer` and publish each row of the CSV file to a specific topic using the Kafka producer. The `BOOTSTRAP_SERVER` should be set as localhost 9092, which is available since we subscribed to it as a local host in our Docker. Additionally, we need to specify how we want to format the customers' data. For instance, we convert the `key` to an string value, then encode it because the Kafka topic expects messages in binary format. To handle this encoding and conversion of our message `value` to the correct format for Kafka, we needed to encode and convert our object as a dictionary into a string first, and then convert it to binary. We must keep in mind the serialisation process because we will need to reverse it in the same order. We also perform customer key serialisation and value serialisation.

`consumer` consumes from a specific set of `KAFKA_TOPIC`, which is the same one that has been used in producer. `auto_offset_reset` defines how consumer will read the topic (earliest/latest). `key_deserializer` and `value_deserializer` are used to decode the binary format to human-readable format since Kafka stores data in binary format.

### Why do we need to define schema?

These messages are defined and formatted in an adjacent manner. The only requirement is that we ensure proper serialisation and deserialization work together. However, in this particular setup, we do not have a specific schema defined. For instance, if the producer, decide to pass data but make changes to the format, such as altering the JSON structure or removing certain identifiers, it will still be printed out in the same way without any issues. This is because we did not specify a schema for validation.

It is crucial for these systems to trust the messages we send out, which is why specifying schemas and using our concept while working with Kafka streaming is essential. Otherwise, this may lead to issues, especially when dealing with production systems. We aim to ensure that our classes and messages are consistent and backward compatible to avoid disrupting any downstream systems that rely on our messages.

### Arvo example

The steps to perform experience are the same in `Json example`. The main difference is using AvroSerializer class, then we don't need to worry about the expected format that Kafka requires.

The contrast between this Json and our example lies in the way we define serialisation and deserialisation, as well as the level of serialisation required in our codebase. This distinction ensures the contract between various services. For instance, if your team is responsible for constructing the messages, while another team is tasked with receiving and interpreting these messages as a consumer.
