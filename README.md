![Logo](https://s3-eu-west-1.amazonaws.com/org.paraio/para.png)
============================

> ### Generic SQL DAO plugin for Para

[![Maven Central Version](https://img.shields.io/maven-central/v/com.erudika/para-dao-sql)](https://central.sonatype.com/artifact/com.erudika/para-dao-sql)
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
WAR file `para-x.y.z.war`. Para will look for plugins inside `lib` and pick up the plugin but the plugin itself will
try to load the correct driver and needs to know where to look for it. That's why you have to set the system property
`loader.path=./lib`.

### Supported database engines

- SQL Server
- PostgreSQL
- MySQL
- MariaDB
- Redshift
- H2 DB
- Oracle
- SQLite

### Configuration

Here are all the configuration properties for this plugin (these go inside your `application.conf`):
```ini
para.sql.driver = "com.mysql.jdbc.Driver"
para.sql.url = "mysql://localhost:3306"
para.sql.user = "user"
para.sql.password = "secret"
```
Finally, set the DAO config property - for H2 DB this must be equal to `H2DAO`, for the other databases it should be:
```ini
para.dao = "SqlDAO"
```
This could be a Java system property or part of a `application.conf` file on the classpath.
This tells Para to use the SqlDAO Data Access Object (DAO) implementation instead of the default.

SQLite, for example, has the simplest configuration:
```ini
para.sql.driver = "org.sqlite.JDBC"
para.sql.url = "sqlite:/home/user/para.db"
```

The environment variable `para.sql.url` is required and provides the URL to connect to the SQL database.
The SQL DAO uses JDBC and will prefix your URL with the JDBC protocol, so you don't need to include the JDBC
protocol in your URL path. For example, to connect to a MySQL server with URL `mysql://localhost:3306`,
the SQL DAO will prefix this URL with the JDBC protocol to form the full URL `jdbc:mysql://localhost:3306`.

The URL you specify should also include in it's path the database to be used by Para. The SQL DAO will not
automatically create a **database** for you (though Para _does_ create **tables** within your database automatically),
so you must use an existing database. For example, you cannot simply specify the URL to your MySQL cluster/server
(`mysql://localhost:3306`), but rather you need to specify the path to an existing database
(`mysql://localhost:3306/para`). Note that the user name and password you provide with `para.sql.user` and
`para.sql.password` should correspond to the specific database you specify in the URL, and that user should have
complete permissions within that database.


### Using a JDBC Driver

The SQL DAO uses JDBC to connect to your SQL database, which means a SQL driver (java.sql.Driver) will be needed for
your chosen flavor of SQL (for example, `com.mysql.jdbc.Driver` is used for MySQL).  You must specify the
fully-qualified class name for your SQL driver. Upon initialization, the SQL DAO will attempt to load this driver
and verify that it exists in the classpath. If the driver cannot be found, the SQL DAO will fail to initiailize and
the DAO cannot be used.

In addition to specifying the driver name, you need to ensure the JAR file containing the SQL driver corresponding to
your database is on your classpath when launching Para Server. The easiest way to do this is to add your SQL driver's
JAR file to the `lib/` directory relative to the location of the Para Server WAR file `para-x.y.z.war`.

### Working with Oracle database

To use Oracle DB you need to create a user (schema) for Para, with `CREATE SESSION` and `CREATE TABLE` privileges.
You also need to enable writes on the USERS tablespace if you get an error like
`ora-01950: no privileges on tablespace 'users'`.

```sql
CREATE USER para IDENTIFIED BY <password>;
GRANT CREATE SESSION, CREATE TABLE TO para;
ALTER USER para quota unlimited on USERS;
```

Then the configuration will look something like this:
```ini
para.sql.driver = "oracle.jdbc.OracleDriver"
para.sql.url = "oracle:thin:@127.0.0.1:1521/XE"
para.sql.user = "para"
para.sql.password = "secret"
```

If you are have a `sysdba/sysoper` type of user, you can set `para.sql.user = "para as sysdba"`.
The plugin has been tested with the Express edition of Oracle 18c database.

### Schema

**BREAKING CHANGE:** The schema has changed in v1.30.0 - columns `timestamp` and `updated` were removed,
column `json_updates` was added. `H2DAO` attempts to apply these changes automatically or error, but `SqlDAO` does not.
**Execute the following statements one after another before switching to the new version:**
```sql
ALTER TABLE {app_identifier} DROP COLUMN timestamp, updated;
ALTER TABLE {app_identifier} ADD json_updates NVARCHAR;
```
This is not required for tables created after v1.30.0.

Here's the schema for each table created by Para:
```sql
CREATE TABLE {app_identifier} (
    id            NVARCHAR NOT NULL,
    type          NVARCHAR,
    name          NVARCHAR,
    parentid      NVARCHAR,
    creatorid     NVARCHAR,
    json          NVARCHAR,
    json_updates  NVARCHAR
)
```

### Dependencies

- [HikariCP](https://github.com/brettwooldridge/HikariCP) for JDBC Connection Pooling
- [Para Core](https://github.com/Erudika/para)

## License
[Apache 2.0](LICENSE)
