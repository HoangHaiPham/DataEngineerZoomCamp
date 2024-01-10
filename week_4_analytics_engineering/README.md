# Goal

Transforming the data loaded in DWH to Analytical Views developing a dbt project.

Slides can be found [here](./Data%20engineering%20Zoomcap%20week%204_%20Analytics%20Engineers.pptx)

# Prerequisites

We will build a project using dbt and a running data warehouse. By this stage of the course you should have already:

- A running warehouse (BigQuery or postgres)
- A set of running pipelines ingesting the project dataset (week 3 completed): [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)
- Yellow taxi data - Years 2019 and 2020
- Green taxi data - Years 2019 and 2020
- fhv data - Year 2019.

_Note: If you recieve an error stating "Permission denied while globbing file pattern." when attemting to run fact_trips.sql [this video](https://www.youtube.com/watch?v=kL3ZVNL9Y4A) may be helpful in resolving the issue._

# Table of contents

- [DE Zoomcamp 4.1.1 - Analytics Engineering Basics](#de-zoomcamp-411---analytics-engineering-basics)
  - [What is analytics engineering?](#what-is-analytics-engineering)
  - [ETL vs ELT](#etl-vs-elt)
  - [Data modeling concepts (fact and dim tables)](#data-modeling-concepts-fact-and-dim-tables)
- [DE Zoomcamp 4.1.2 - What is dbt](#de-zoomcamp-412---what-is-dbt)
- [DE Zoomcamp 4.2.1 - Start Your dbt Project: BigQuery and dbt Cloud (Alternative A)](#de-zoomcamp-421---start-your-dbt-project-bigquery-and-dbt-cloud-alternative-a)
- [DE Zoomcamp 4.2.2 - Start Your dbt Project: Postgres and dbt Core Locally (Alternative B)](#de-zoomcamp-422---start-your-dbt-project-postgres-and-dbt-core-locally-alternative-b)

# [DE Zoomcamp 4.1.1 - Analytics Engineering Basics](https://www.youtube.com/watch?v=uF76d5EmdtU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=35)

### What is analytics engineering?

As the _data domain_ has developed over time, new tools have been introduced that have changed the dynamics of working with data:

1. Massively parallel processing (MPP) databases
   - Lower the cost of storage
   - BigQuery, Snowflake, Redshift...
2. Data-pipelines-as-a-service
   - Simplify the ETL process
   - Fivetran, Stitch...
3. SQL-first / Version control systems
   - Looker...
4. Self service analytics
   - Mode...
5. Data governance

The introduction of all of these tools changed the way the data teams work as well as the way that the stakeholders consume the data, creating a gap in the roles of the data team. Traditionally:

- The **_data engineer_** prepares and maintains the infrastructure the data team needs.
- The **_data analyst_** uses data to answer questions and solve problems (they are in charge of _today_).
- The **_data scientist_** predicts the future based on past patterns and covers the what-ifs rather than the day-to-day (they are in charge of _tomorrow_).

However, with the introduction of these tools, both data scientists and analysts find themselves writing more code even though they're not software engineers and writing code isn't their top priority. Data engineers are good software engineers but they don't have the training in how the data is going to be used by the business users.

The **_analytics engineer_** is the role that tries to fill the gap: it introduces the good software engineering practices to the efforts of data analysts and data scientists. The analytics engineer may be exposed to the following tools:

1. Data Loading (Stitch...)
2. Data Storing (Data Warehouses)
3. Data Modeling (dbt, Dataform...)
4. Data Presentation (BI tools like Looker, Mode, Tableau...)

This lesson focuses on the last 2 parts: Data Modeling and Data Presentation.

### ETL vs ELT

![etl-vs-elt](./images/etl-vs-elt.png)

### Data modeling concepts (fact and dim tables)

[Ralph Kimball's Dimensional Modeling](<https://www.wikiwand.com/en/Dimensional_modeling#:~:text=Dimensional%20modeling%20(DM)%20is%20part,use%20in%20data%20warehouse%20design.>) is an approach to Data Warehouse design which focuses on 2 main points:

- Deliver data which is understandable to the business users.
- Deliver fast query performance.

Other goals such as reducing redundant data (prioritized by other approaches such as [3NF](<https://www.wikiwand.com/en/Third_normal_form#:~:text=Third%20normal%20form%20(3NF)%20is,integrity%2C%20and%20simplify%20data%20management.>) by [Bill Inmon](https://www.wikiwand.com/en/Bill_Inmon)) are secondary to these goals. Dimensional Modeling also differs from other approaches to Data Warehouse design such as [Data Vaults](https://www.wikiwand.com/en/Data_vault_modeling).

Dimensional Modeling is based around 2 important concepts:

- **_Fact Table_**:
  - _Facts_ = _Measures_
  - Typically numeric values which can be aggregated, such as measurements or metrics.
    - Examples: sales, orders, etc.
  - Corresponds to a [_business process_ ](https://www.wikiwand.com/en/Business_process).
  - Can be thought of as _"verbs"_.
- **_Dimension Table_**:
  - _Dimension_ = _Context_
  - Groups of hierarchies and descriptors that define the facts.
    - Example: customer, product, etc.
  - Corresponds to a _business entity_.
  - Can be thought of as _"nouns"_.
- Dimensional Modeling is built on a [**_star schema_**](https://www.wikiwand.com/en/Star_schema) with fact tables surrounded by dimension tables.

A good way to understand the _architecture_ of Dimensional Modeling is by drawing an analogy between dimensional modeling and a restaurant:

- Stage Area:
  - Contains the raw data.
  - Not meant to be exposed to everyone.
  - Similar to the food storage area in a restaurant.
- Processing area:
  - From raw data to data models.
  - Focuses in efficiency and ensuring standards.
  - Similar to the kitchen in a restaurant.
- Presentation area:
  - Final presentation of the data.
  - Exposure to business stakeholder.
  - Similar to the dining room in a restaurant.

# [DE Zoomcamp 4.1.2 - What is dbt](https://www.youtube.com/watch?v=4eCouvVOJUw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=36)

**`dbt`** stands for **`data build tool`**. It's a `transformation` tool: it allows us to transform process `raw` data in our Data Warehouse to `transformed` data which can be later used by Business Intelligence tools and any other data consumers.

dbt also allows us to introduce good software engineering practices by defining a `deployment workflow`:

1. Develop models.
2. Test and document models.
3. Deploy models with `version control` and `CI/CD`.

### How does dbt work?

dbt works by defining a **`modeling layer`** that sits on top of our Data Warehouse. The modeling layer will turn `tables` into **`models`** which we will then transform into `derived models`, which can be then stored into the Data Warehouse for persistence.

A **`model`** is a .sql file with a `SELECT` statement; no DDL (Data Definition Language) or DML (Data Manipulation Language) is used. dbt will compile the file and run it in our Data Warehouse.

### How to use dbt?

dbt has 2 main components: `dbt Core` and `dbt Cloud`:

- **`dbt Core`**: open-source project that allows the data transformation.
  - Builds and runs a dbt project (.sql and .yaml files).
  - Includes SQL compilation logic, macros and database adapters.
  - Includes a CLI interface to run dbt commands locally.
  - Open-source and free to use.
- **`dbt Cloud`**: SaaS application to develop and manage dbt projects.
  - Web-based IDE to develop, run and test a dbt project.
  - Jobs orchestration.
  - Logging and alerting.
  - Intregrated documentation.
  - Free for individuals (one developer seat).

For integration with BigQuery we will use the dbt Cloud IDE, so a local installation of dbt core isn't required. For developing locally rather than using the Cloud IDE, dbt Core is required. Using dbt with a local Postgres database can be done with dbt Core, which can be installed locally and connected to Postgres and run models through the CLI.

![dbt](./images/dbt.png)

# [DE Zoomcamp 4.2.1 - Start Your dbt Project: BigQuery and dbt Cloud (Alternative A)](https://www.youtube.com/watch?v=iMxh6s_wL4Q&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=37)

In order to use dbt Cloud you will need to create a user account. Got to the [dbt homepage](https://www.getdbt.com/) and sign up.

During the sign up process you will be asked to create a starter project and connect to a database. We will connect dbt to BigQuery using [BigQuery OAuth](https://docs.getdbt.com/docs/dbt-cloud/cloud-configuring-dbt-cloud/cloud-setting-up-bigquery-oauth). More detailed instructions on how to generate the credentials and connect both services are available [in dbt_cloud_setup.md](./dbt_cloud_setup.md). When asked, connnect the project to your `development` dataset.

Make sure that you set up a GitHub repo for your project. In `Account settings` > `Projects` you can select your project and change its settings, such as `Name` or `dbt Project Subdirectoy`, which can be convenient if your repo is previously populated and would like to keep the dbt project in a single subfolder.

**Go to BigQuery and create schema [`dbt_hpham`](./images/development-credential.png).**

- This schema is where we create models while we are developing them. Quite similar to a sandbox.

**Go to BigQuery and create schema `production`.**

- This schema is used for running the models after deployment.

In the IDE windows, press the green `Initilize` button to create the project files. Inside `dbt_project.yml`, change the project name both in the `name` field as well as right below the `models:` block. You may comment or delete the `example` block at the end.

# [DE Zoomcamp 4.2.2 - Start Your dbt Project: Postgres and dbt Core Locally (Alternative B)](https://www.youtube.com/watch?v=1HmL63e-vRs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=37)

As an alternative to the cloud, that require to have a cloud database, you will be able to run the project installing dbt locally. You can follow the [official dbt documentation](https://docs.getdbt.com/docs/core/installation-overview) or use a docker image from [oficial dbt repo](https://github.com/dbt-labs/dbt-core). You will need to install the latest version (1.0) with the postgres adapter (dbt-postgres). After local installation you will have to set up the connection to PG in the `profiles.yml`, you can find the templates [here](https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup).
