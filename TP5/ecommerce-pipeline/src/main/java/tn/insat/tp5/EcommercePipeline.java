package tn.insat.tp5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import java.util.Iterator;

public class EcommercePipeline {

    static StructType EVENT_SCHEMA = new StructType(new StructField[]{
        new StructField("user_id", DataTypes.StringType, true, Metadata.empty()),
        new StructField("event_type", DataTypes.StringType, true, Metadata.empty()),
        new StructField("category", DataTypes.StringType, true, Metadata.empty()),
        new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("timestamp", DataTypes.LongType, true, Metadata.empty())
    });

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
            .appName("EcommercePipeline").getOrCreate();

        // 1. Lecture du flux Kafka
        Dataset<Row> raw = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "ecommerce-events")
            .load();

        // 2. Désérialisation JSON
        Dataset<Row> events = raw
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), EVENT_SCHEMA).as("data"))
            .select("data.*");

        // 3. Filtrer uniquement les achats
        Dataset<Row> purchases = events
            .filter(col("event_type").equalTo("purchase"));

        // 4. Agrégation par catégorie sur fenêtre de 1 minute
        Dataset<Row> aggregated = purchases
            .withColumn("event_ts",
                to_timestamp(col("timestamp").divide(1000)))
            .withWatermark("event_ts", "2 minutes")
            .groupBy(window(col("event_ts"), "1 minute"), col("category"))
            .agg(
                sum("amount").as("total_amount"),
                count("*").as("purchase_count"));

        // 5. Écriture dans HBase via foreachBatch
        StreamingQuery hbaseQuery = aggregated.writeStream()
            .outputMode("update")
            .foreachBatch((Dataset<Row> batch, Long batchId) -> {
                batch.foreachPartition((Iterator<Row> rows) -> {
                    Configuration conf = HBaseConfiguration.create();
                    try (Connection conn = ConnectionFactory.createConnection(conf);
                         Table table = conn.getTable(TableName.valueOf("sales"))) {
                        while (rows.hasNext()) {
                            Row row = rows.next();
                            String cat = row.getString(row.fieldIndex("category"));
                            double total = row.getDouble(row.fieldIndex("total_amount"));
                            long cnt = row.getLong(row.fieldIndex("purchase_count"));

                            Put put = new Put(Bytes.toBytes(cat));
                            put.addColumn(Bytes.toBytes("stats"),
                                Bytes.toBytes("total_amount"),
                                Bytes.toBytes(String.valueOf(total)));
                            put.addColumn(Bytes.toBytes("stats"),
                                Bytes.toBytes("purchase_count"),
                                Bytes.toBytes(String.valueOf(cnt)));
                            put.addColumn(Bytes.toBytes("meta"),
                                Bytes.toBytes("last_update"),
                                Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                            table.put(put);
                        }
                    }
                });
            })
            .trigger(Trigger.ProcessingTime("30 seconds"))
            .start();

        // 6. Archivage HDFS de TOUS les événements (format JSON)
        StreamingQuery hdfsQuery = events.writeStream()
            .outputMode("append")
            .format("json")
            .option("path", "hdfs://hadoop-master:9000/data/ecommerce/raw")
            .option("checkpointLocation", "/tmp/checkpoint-hdfs")
            .trigger(Trigger.ProcessingTime("1 minute"))
            .start();

        hbaseQuery.awaitTermination();
    }
}