import java.util.Date;
import java.util.Properties;
import java.util.Random;

import com.toshiba.mwcloud.gs.Aggregation;
import com.toshiba.mwcloud.gs.AggregationResult;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import com.toshiba.mwcloud.gs.Query;
import com.toshiba.mwcloud.gs.RowKey;
import com.toshiba.mwcloud.gs.RowSet;
import com.toshiba.mwcloud.gs.TimeSeries;
import com.toshiba.mwcloud.gs.TimeUnit;
import com.toshiba.mwcloud.gs.TimestampUtils;

/**
 * Industrial IoT sensor pipeline on GridDB.
 *
 * Demonstrates: schema-as-class, a per-device TimeSeries container,
 * a streaming ingest loop with inline anomaly tagging, time-range
 * queries, server-side aggregation, and a TQL filter.
 *
 * Requires a running GridDB cluster (default: cluster "myCluster" on
 * 127.0.0.1:10001) and the GridStore client on the classpath:
 *
 *   <dependency>
 *     <groupId>com.github.griddb</groupId>
 *     <artifactId>gridstore</artifactId>
 *     <version>5.5.0</version>   <!-- check Maven Central for latest -->
 *   </dependency>
 */
public class SensorPipeline {

    /** Container schema. The @RowKey on a Date is required for a TimeSeries. */
    public static class SensorReading {
        @RowKey Date timestamp;
        double temperature;   // degrees Celsius
        double vibration;     // mm/s RMS
        String status;        // "OK" or "ALERT"
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("notificationMember", "127.0.0.1:10001");
        props.setProperty("clusterName", "myCluster");
        props.setProperty("user", "admin");
        props.setProperty("password", "admin");

        GridStore store = GridStoreFactory.getInstance().getGridStore(props);
        try {
            TimeSeries<SensorReading> turbine =
                    store.putTimeSeries("turbine_01", SensorReading.class);

            ingest(turbine);
            printLastHour(turbine);
            printAggregates(turbine);
            printAlerts(turbine);
        } finally {
            store.close();
        }
    }

    /** Write 60 minute-spaced readings, tagging high-vibration rows as ALERT. */
    private static void ingest(TimeSeries<SensorReading> turbine) throws Exception {
        Random rand = new Random();
        long start = System.currentTimeMillis();

        for (int i = 0; i < 60; i++) {
            SensorReading r = new SensorReading();
            r.timestamp   = new Date(start + i * 60_000L);
            r.temperature = 70 + rand.nextDouble() * 10;
            r.vibration   = rand.nextDouble() * 5;
            r.status      = r.vibration > 4.0 ? "ALERT" : "OK";
            turbine.put(r);
        }
        System.out.println("Ingested 60 readings into turbine_01.");
    }

    private static void printLastHour(TimeSeries<SensorReading> turbine) throws Exception {
        Date end   = TimestampUtils.current();
        Date begin = TimestampUtils.add(end, -1, TimeUnit.HOUR);

        RowSet<SensorReading> rows = turbine.query(begin, end).fetch();
        System.out.println("\n--- Readings (last hour) ---");
        while (rows.hasNext()) {
            SensorReading r = rows.next();
            System.out.printf("%s  temp=%.1f  vib=%.2f  %s%n",
                    TimestampUtils.format(r.timestamp),
                    r.temperature, r.vibration, r.status);
        }
    }

    private static void printAggregates(TimeSeries<SensorReading> turbine) throws Exception {
        Date end   = TimestampUtils.current();
        Date begin = TimestampUtils.add(end, -1, TimeUnit.HOUR);

        AggregationResult avg  = turbine.aggregate(begin, end, "temperature", Aggregation.AVERAGE);
        AggregationResult peak = turbine.aggregate(begin, end, "vibration", Aggregation.MAXIMUM);

        System.out.println("\n--- Aggregates (last hour) ---");
        System.out.println("Average temperature: " + avg.getDouble());
        System.out.println("Peak vibration:      " + peak.getDouble());
    }

    private static void printAlerts(TimeSeries<SensorReading> turbine) throws Exception {
        Query<SensorReading> q = turbine.query("SELECT * WHERE vibration > 4.0");
        RowSet<SensorReading> alerts = q.fetch();
        System.out.println("\n--- TQL alert filter ---");
        System.out.println("Alert readings: " + alerts.size());
    }
}
