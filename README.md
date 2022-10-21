# Sparkify: Data Modeling with Cassandra
Sparkify is a startup with a music streaming app. Data scientists at Sparkify are particularly interested in understanding what songs users are listening to.

The goal of this project is to define a data model that will help our data scientists answer questions about user activity. In this case, we are working with a NoSQL database, Apache Cassandra, which means we have to be especially intentional in our data modeling. We first need to understand what queries our data scientists would like to run, and then we can design tables to fit those queries. We will be aiming for "1 query, 1 table".

# File Overview

- event_data/ - contains our data for user sessions in the streaming app in csv format. files are partitioned by day. (e.g. 2018-11-05-events.csv)
- CQL_quries.py contains the create and insdert into tables CQL quries 
- CreateTables.py Creates a connection to database and creates all tables 
- ETLDesign.ipynb Contains the design and the test of the ETL process to ensure we can run the given quries
- ETL.py Implement the ETL process which has designed in ETLDesign.ipynb
To run this project you simply should run two commands in order in command prompt
- python CreateTables.py
- python ETL.py
