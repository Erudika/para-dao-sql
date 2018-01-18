![Logo](https://s3-eu-west-1.amazonaws.com/org.paraio/para.png)
============================

> ### Generic SQL DAO plugin for Para

[![Build Status](https://travis-ci.org/Erudika/para-dao-sql.svg?branch=master)](https://travis-ci.org/Erudika/para-dao-sql)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.erudika/para-dao-sql/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.erudika/para-dao-sql)
[![Join the chat at https://gitter.im/Erudika/para](https://badges.gitter.im/Erudika/para.svg)](https://gitter.im/Erudika/para?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## What is this?

**Para** was designed as a simple and modular back-end framework for object persistence and retrieval.
It enables your application to store objects directly to a data store (NoSQL) or any relational database (RDBMS)
and it also automatically indexes those objects and makes them searchable.

This plugin allows Para to store data in virtually any SQL database.

## Documentation

### [Read the Docs](https://paraio.org/docs)

## Getting started

The plugin is on Maven Central. Here's the Maven snippet to include in your `pom.xml`:

```xml
<dependency>
  <groupId>com.erudika</groupId>
  <artifactId>para-dao-sql</artifactId>
  <version>{see_green_version_badge_above}</version>
</dependency>
```

Alternatively you can download the JAR from the "Releases" tab above put it in a `lib` folder alongside the server
WAR file `para-x.y.z.war`. Para will look for plugins inside `lib` and pick up the plugin.

### Configuration

Here are all the configuration properties for this plugin (these go inside your `application.conf`):
```ini
para.sql.url = "mysql://localhost:3306"
para.sql.user = "user"
para.sql.password = "secret"
```
Finally, set the config property:
```
para.dao = "SqlDAO"
```
This could be a Java system property or part of a `application.conf` file on the classpath.
This tells Para to use the SqlDAO Data Access Object (DAO) implementation instead of the default.

#### Setting the SQL URL
The environment variable ```para.sql.url``` provides the URL to connect to the SQL database. The SQL DAO uses JDBC and 
will prefix your URL with the JDBC protocol. For example, to connect to a MySQL server with URL ```mysql://localhost:3306```,
The SQL DAO will prefix this URL with the JDBC protocol to form the full URL ```jdbc:mysql://localhost:3306```.


#### Configuring a SQL Driver
The SQL DAO uses JDBC to connect to your SQL database, which means a SQL driver (java.sql.Driver) will be needed for 
your chosen flavor of SQL (for example, ```com.mysql.jdbc.Driver``` is used for MySQL). The version of JDBC used in 
the SQL DAO will automatically detect SQL Drivers in your classpath when launching Para Server, so all you need to
do is download the jarfile containing the SQL driver corresponding to your database and ensure it's part of the
classpath when launching Para Server.

There is no need to directly specify the SQL driver as part of your configuration, as JDBC will infer
the driver used by the protocol specified in your URL.

### Dependencies

- [HikariCP](https://github.com/brettwooldridge/HikariCP) for JDBC Connection Pooling 
- [Para Core](https://github.com/Erudika/para)

## License
[Apache 2.0](LICENSE)
