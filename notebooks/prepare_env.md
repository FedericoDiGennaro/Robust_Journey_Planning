---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.5
  kernelspec:
    display_name: Bash
    language: bash
    name: bash
---

# Prepare environment

You must prepare your environment __if you have not done so in the previous exercises__ in order to do this homework. Follow the steps below.


-----
1. Run the next cell, and verify that you are logged as you, and not as someone else

```bash
echo "You are ${USERNAME:-nobody}"
```

-----
2. Run the code below and verify the existance of your database. Execute step 3 if you have a database.\
Otherwise go to step 6, if it shows an empty list as shown below:

```
    +-----------------+
    |  database_name  |
    +-----------------+
    +-----------------+
```

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW DATABASES LIKE '${USERNAME}';"
```

-----
3. Review the content of your database if you have one.

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW TABLES IN ${USERNAME};"
```

-----
4. Drop your database after having reviewed its content in step 3, and __you are ok losing its content__.

```bash
beeline -u "${HIVE_JDBC_URL}" -e "DROP DATABASE IF EXISTS ${USERNAME} CASCADE;"
```

-----
5. Verify that you the database is gone

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW DATABASES LIKE '${USERNAME}';"
```

-----
6. Run the remaining cells to reconstruct your hive folder and reconfigure ACL permissions on HDFS

```bash
hdfs dfs -ls "/user/${USERNAME}"
```

```bash
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/hive"
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/.Trash"
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/ER"
hdfs dfs -rm -r -f -skipTrash "tmp/ER_data.zip"
```

```bash
hdfs dfs -mkdir -p                                /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl    -m group::r-x                /user/${USERNAME:-nobody}
hdfs dfs -setfacl    -m other::---                /user/${USERNAME:-nobody}
hdfs dfs -setfacl    -m default:group::r-x        /user/${USERNAME:-nobody}
hdfs dfs -setfacl    -m default:other::---        /user/${USERNAME:-nobody}
hdfs dfs -setfacl -R -m group::r-x                /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl -R -m other::---                /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl -R -m default:group::r-x        /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl -R -m default:other::---        /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl    -m user:hive:rwx             /user/${USERNAME:-nobody}/hive
hdfs dfs -setfacl    -m default:user:hive:rwx     /user/${USERNAME:-nobody}/hive
```

-----
7. Recreate the __external__ tables `sbb_orc` and `sbb_05_11_2018` which you will need in the homework.

```bash
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
CREATE DATABASE IF NOT EXISTS ${USERNAME:-nobody} LOCATION '/user/${USERNAME:-nobody}/hive';

USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.sbb_orc;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.sbb_orc(
        betriebstag STRING,
        fahrt_bezichner STRING,
        betreiber_id STRING,
        betreiber_abk STRING,
        betreiber_name STRING,
        produkt_id STRING,
        linien_id STRING,
        linien_TEXT STRING,
        umlauf_id STRING,
        verkehrsmittel_text STRING,
        zusatzfahrt_tf STRING,
        faellt_aus_tf STRING,
        bpuic STRING,
        haltestellen_name STRING,
        ankunftszeit STRING,
        an_prognose STRING,
        an_prognose_status STRING,
        abfahrtszeit STRING,
        ab_prognose STRING,
        ab_prognose_status STRING,
        durchfahrt_tf STRING
    )
    PARTITIONED BY (year INTEGER, month INTEGER)
    STORED AS ORC
    LOCATION '/data/sbb/part_orc/istdaten';
    
    MSCK REPAIR TABLE sbb_orc;
    "
```

```bash
hdfs dfs -ls /data/sbb/part_orc/istdaten
```

```bash
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "

USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.allstops_orc;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.allstops_orc(
        stop_id STRING,
        stop_name STRING,
        stop_lat FLOAT,
        stop_lon FLOAT,
        location_type STRING,
        parent_station STRING
    )

    STORED AS ORC
    LOCATION '/data/sbb/orc/allstops';
    MSCK REPAIR TABLE allstops_orc;
    "
```

```bash
# run this cell to download the data (zip folder) from the specified link
# week of interest starting from Tuesday December 9, 2020, 9:36 AM (UTC+01:00)
curl -o "/tmp/ER_data.zip" "https://opentransportdata.swiss/dataset/6f55f96d-7644-4901-b927-e9cf05a8c7f0/resource/5a12e34f-7536-4648-885c-f9febf9f6e0c/download/gtfs_fp2020_2020-12-09.zip"
```

```bash
hdfs dfs -mkdir /user/${USERNAME}/ER
```

```bash
# run this cell to copy the data in the folder 'ER'
hdfs dfs -copyFromLocal /tmp/ER_data.zip /user/${USERNAME}/ER
```

```bash
hdfs dfs -du -h -s /user/${USERNAME}/ER/ER_data.zip
```

```bash
# Retrieve the ZIP file to a local directory
hadoop fs -get /user/${USERNAME}/ER/ER_data.zip /tmp/

# Extract the contents of the ZIP file
unzip /tmp/ER_data.zip -d /tmp/extracted/
```

```bash
# run this cell to copy the file in a new folder 
hdfs dfs -copyFromLocal /tmp/extracted /user/${USERNAME}/ER
```

```bash
hdfs dfs -ls /user/${USERNAME}/ER/
```

```bash
# now let's create a folder for each txt file. this will make it easier to right queries on them 
# and hence create tables.
```

```bash
# run this cell to create the desired empty directories
hdfs dfs -mkdir /user/${USERNAME}/ER/agency
hdfs dfs -mkdir /user/${USERNAME}/ER/calendar
hdfs dfs -mkdir /user/${USERNAME}/ER/calendar_dates
hdfs dfs -mkdir /user/${USERNAME}/ER/feed_info
hdfs dfs -mkdir /user/${USERNAME}/ER/routes
hdfs dfs -mkdir /user/${USERNAME}/ER/stop_times
hdfs dfs -mkdir /user/${USERNAME}/ER/stops
hdfs dfs -mkdir /user/${USERNAME}/ER/transfers
hdfs dfs -mkdir /user/${USERNAME}/ER/trips
```

```bash
# run this cell to copy each txt file in the apposite folder inside ER
hdfs dfs -cp /user/${USERNAME}/ER/extracted/agency.txt /user/${USERNAME}/ER/agency/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/calendar.txt /user/${USERNAME}/ER/calendar/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/calendar_dates.txt /user/${USERNAME}/ER/calendar_dates/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/feed_info.txt /user/${USERNAME}/ER/feed_info/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/routes.txt /user/${USERNAME}/ER/routes/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/stop_times.txt /user/${USERNAME}/ER/stop_times/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/stops.txt /user/${USERNAME}/ER/stops/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/transfers.txt /user/${USERNAME}/ER/transfers/
hdfs dfs -cp /user/${USERNAME}/ER/extracted/trips.txt /user/${USERNAME}/ER/trips/
```

```bash
hdfs dfs -ls /user/${USERNAME}/ER
```

```bash
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/ER/extracted"
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/.Trash"
```

```bash
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/ER/ER_data.zip"
hdfs dfs -rm -r -f -skipTrash "/user/${USERNAME:-nobody}/.Trash"
```

```bash
# create the stop_times table
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.stop_times;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.stop_times(
        trip_id STRING,
        arrival_time STRING,
        departure_time STRING,
        stop_id STRING,
        stop_sequence STRING,
        pickup_type STRING,
        drop_off_type STRING
        )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/${USERNAME}/ER/stop_times';
"
```

```bash
# create the transfers table
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.transfers;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.transfers(
        from_stop_id STRING,
        to_stop_id STRING,
        transfer_type STRING,
        min_transfer_time STRING
        )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/${USERNAME}/ER/transfers';
"
```

```bash
# create the stops table
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.stops;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.stops(
        stop_id STRING,
        stop_name STRING,
        stop_lat STRING,
        stop_lon STRING,
        location_type STRING,
        parent_station STRING
        )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/${USERNAME}/ER/stops';
"
```

```bash
# create the calendar table
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.calendar;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.calendar(
        service_id STRING,
        monday STRING,
        tuesday STRING,
        wednesday STRING,
        thursday STRING,
        friday STRING,
        saturday STRING,
        sunday STRING,
        start_date STRING,
        end_date STRING
        )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC
    LOCATION '/user/${USERNAME}/ER/calendar';
"
```

```bash
# create the trips table
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.trips;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.trips(
        route_id STRING,
        service_id STRING,
        trip_id STRING,
        trip_headsign STRING,
        trip_short_name STRING,
        direction_id STRING,
        block_id STRING,
        shape_id STRING
        )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS ORC
    LOCATION '/user/${USERNAME}/ER/trips';
"
```

```bash
# create the stops table
beeline -u "${HIVE_JDBC_URL}" --silent=true -e "
USE ${USERNAME:-nobody};

DROP TABLE IF EXISTS ${USERNAME:-nobody}.routes;

CREATE EXTERNAL TABLE ${USERNAME:-nobody}.routes(
        route_id STRING,
        agency_id STRING,
        route_short_name STRING,
        route_long_name STRING,
        route_desc STRING,
        route_type STRING
        )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/${USERNAME}/ER/routes';
"
```

```bash
beeline -u "${HIVE_JDBC_URL}" -e "SHOW TABLES IN ${USERNAME};"
```

```bash
hdfs dfs -cat /user/${USERNAME}/ER/transfers/transfers.txt | head -n 100
```

```bash

```
