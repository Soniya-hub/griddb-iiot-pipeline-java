# Building an Industrial IoT Sensor Pipeline with GridDB and Java

Industrial equipment is chatty. A single turbine, pump, or CNC machine can emit temperature, vibration, pressure, and status readings every few seconds, and a factory floor might run hundreds of them in parallel. The result is a relentless, ordered stream of timestamped measurements — the classic *time-series* workload. Storing that stream in a general-purpose relational database works until it doesn't: indexes bloat, inserts slow down under sustained load, and range scans over millions of rows start timing out right when you need them for a dashboard or an alert.

This is the shape of problem [GridDB](https://www.griddb.net/) is tuned for. Originally developed by Toshiba and now open source, it leans on an in-memory-first engine and a data model that treats timestamped records as a first-class citizen rather than just another set of rows. In this tutorial we'll model a sensor stream, push simulated readings into it, and run the kind of time-bounded and aggregate queries an industrial monitoring system leans on day to day — all in plain Java, which is the database's native client language.

## Why GridDB for time-series data

GridDB's data model is built around two ideas that make it a strong fit for IoT.

The first is the **key-container model**. Instead of one giant table holding readings from every device, GridDB encourages one *container* per data source. Each turbine gets its own time-series container, which keeps related data physically together and lets the engine partition and parallelize across thousands of containers without contention.

The second is the **TimeSeries container** itself — a specialized container type whose row key *must* be a timestamp. Since the engine already knows the rows arrive in chronological order, it can store them compactly, fill in missing points where needed, and resolve range and aggregate queries without walking the entire dataset. The upshot: you get the behaviour of a purpose-built time-series store while still working with ordinary Java objects.

## Setting up

GridDB's Community Edition targets Linux, and you can bring up a node either from the official Docker image or from the `.deb`/`.rpm` packages; the [GitHub quickstart](https://github.com/griddb/griddb) covers starting a node and joining it to a cluster. Once it's up and you've picked a cluster name (we'll use `myCluster`), the only thing your application needs is the Java client on its classpath.

If you're using Maven, add the GridStore client dependency:

```xml
<dependency>
    <groupId>com.github.griddb</groupId>
    <artifactId>gridstore</artifactId>
    <!-- Check Maven Central for the latest 5.x release -->
    <version>5.5.0</version>
</dependency>
```

That's the entire setup. No ORM, no driver registration — just a JAR.

## Modeling the sensor schema

In GridDB, a container's schema is just a Java class. You mark one field with `@RowKey`, and for a TimeSeries container that field has to be a `java.util.Date` (the timestamp). Here's our sensor reading:

```java
import java.util.Date;
import com.toshiba.mwcloud.gs.RowKey;

public class SensorReading {
    @RowKey Date timestamp;
    double temperature;   // degrees Celsius
    double vibration;     // mm/s RMS
    String status;        // "OK" or "ALERT"
}
```

No annotations on the other fields, no separate DDL file. The class *is* the schema.

## Connecting and creating the container

You connect by handing a `Properties` object to `GridStoreFactory`. The properties point the client at your cluster's notification member and supply credentials:

```java
import java.util.Properties;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.TimeSeries;

Properties props = new Properties();
props.setProperty("notificationMember", "127.0.0.1:10001");
props.setProperty("clusterName", "myCluster");
props.setProperty("user", "admin");
props.setProperty("password", "admin");

GridStore store = GridStoreFactory.getInstance().getGridStore(props);

// Create the container if it doesn't exist, then return a handle to it.
TimeSeries<SensorReading> turbine =
        store.putTimeSeries("turbine_01", SensorReading.class);
```

`putTimeSeries` is idempotent: it creates the container the first time and simply returns it on subsequent calls, so your ingestion code doesn't need a separate "create table" step.

## Ingesting simulated sensor data

Real sensors would push data over MQTT, a REST endpoint, or a message broker. To keep the example self-contained, we'll generate a minute-by-minute stream and write each reading directly into the container. Notice that we flag any reading with high vibration as an `ALERT` — a tiny bit of edge logic that mirrors how real predictive-maintenance pipelines tag anomalies at ingest time.

```java
import java.util.Date;
import java.util.Random;

Random rand = new Random();
long start = System.currentTimeMillis();

for (int i = 0; i < 60; i++) {
    SensorReading r = new SensorReading();
    r.timestamp   = new Date(start + i * 60_000L);   // one reading per minute
    r.temperature = 70 + rand.nextDouble() * 10;     // 70–80 C
    r.vibration   = rand.nextDouble() * 5;           // 0–5 mm/s
    r.status      = r.vibration > 4.0 ? "ALERT" : "OK";

    turbine.put(r);   // GridDB uses the timestamp field as the row key
}
```

Each `put` is keyed by the timestamp, so readings land in time order automatically.

## Querying a time range

The payoff of a time-series container is how cleanly it answers "what happened between X and Y?" GridDB gives you a typed range query and `TimestampUtils` for building the bounds:

```java
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;

Date end   = TimestampUtils.current();
Date begin = TimestampUtils.add(end, -1, TimeUnit.HOUR);   // last hour

RowSet<SensorReading> rows = turbine.query(begin, end).fetch();
while (rows.hasNext()) {
    SensorReading r = rows.next();
    System.out.printf("%s  temp=%.1f  vib=%.2f  %s%n",
            TimestampUtils.format(r.timestamp),
            r.temperature, r.vibration, r.status);
}
```

There's no SQL string and no manual `WHERE timestamp BETWEEN ...` — the container is time-aware, so the range is a first-class argument.

## Aggregating for dashboards and alerts

Most monitoring views don't want raw rows; they want a rolled-up number. TimeSeries containers expose built-in aggregations that run server-side, so you don't pull a million rows across the network just to average them:

```java
import com.toshiba.mwcloud.gs.Aggregation;
import com.toshiba.mwcloud.gs.AggregationResult;

AggregationResult avg =
        turbine.aggregate(begin, end, "temperature", Aggregation.AVERAGE);
System.out.println("Average temperature (last hour): " + avg.getDouble());

AggregationResult peak =
        turbine.aggregate(begin, end, "vibration", Aggregation.MAXIMUM);
System.out.println("Peak vibration (last hour): " + peak.getDouble());
```

For anything more selective, GridDB also speaks **TQL**, a SQL-like query language. Pulling just the alert events is a one-liner:

```java
import com.toshiba.mwcloud.gs.Query;

Query<SensorReading> q = turbine.query("SELECT * WHERE vibration > 4.0");
RowSet<SensorReading> alerts = q.fetch();
System.out.println("Alert readings: " + alerts.size());
```

## Wrapping up

In well under a hundred lines of Java, we've stood up a working industrial-IoT pipeline: a typed schema, a TimeSeries container per device, a streaming ingest loop with inline anomaly tagging, and both range and aggregation queries ready for a dashboard or alerting layer. The reason it stays this compact is that GridDB's model lines up with the shape of the data — time-ordered readings from many independent sources — instead of fighting it.

From here, the natural next steps are to fan out to multiple containers (one per machine), wire the ingest side to a real broker like MQTT or Kafka, and surface the aggregation results through a Spring Boot REST API for a live monitoring front end. But the core data layer — the part that usually breaks first at scale — is already handled.

---

*Code for this tutorial is available on GitHub: `https://github.com/Soniya-hub/griddb-iiot-pipeline-java/tree/main`.*
