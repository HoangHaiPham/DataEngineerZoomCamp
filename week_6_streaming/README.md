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
  - [What makes Kafka special?](#what-makes-kafka-special)
  - [Stream processing in real-world scenarios](#stream-processing-in-real-world-scenarios)

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
