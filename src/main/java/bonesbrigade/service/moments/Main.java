package bonesbrigade.service.moments;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

public class Main {

    public static void main(String[] args) {
        final String brokers = System.getenv("KAFKA_BROKERS");
        final String intopic = System.getenv("KAFKA_IN_TOPIC");
        final String outtopic = System.getenv("KAFKA_OUT_TOPIC");

        SparkSession spark = SparkSession
                .builder()
                .appName("KafkaSparkOpenshiftMoments")
                .getOrCreate();

        /* configure the operations to read the input topic */
        Dataset<Row> records = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", intopic)
                .option("failOnDataLoss", false)
                .load()
                .select(functions.column("key"), functions.column("value").cast(DataTypes.StringType).alias("value"));


        MapGroupsWithStateFunction<String, Row, Tuple3<Integer, Double, Double>, Row> stateUpdateFunc =
                (MapGroupsWithStateFunction<String, Row, Tuple3<Integer, Double, Double>, Row>) (s, iterator, groupState) -> {

                    Tuple3<Integer, Double, Double> aggregate;

                    if (groupState.exists()) {
                        aggregate = groupState.get();
                    } else {
                        aggregate = new Tuple3<>(0, 0.0, 0.0);
                    }

                    while (iterator.hasNext()) {
                        Row row = iterator.next();
                        double val = Double.parseDouble(row.getString(1));
                        int count = 1 + aggregate._1();
                        double delta = val - aggregate._2();
                        double mean = aggregate._2() + delta / (double) count;
                        double delta2 = val - mean;
                        double squaredDistance = aggregate._3() + delta * delta2;
                        aggregate = new Tuple3<>(count, mean, squaredDistance);
                    }
                    groupState.update(aggregate);
                    Double variance = aggregate._3() / (double) aggregate._1();
                    return RowFactory.create("{\"mean\":" + aggregate._2().toString() + ", \"variance\":" + variance.toString() + "}");
                };

        StructType structType = new StructType();
        structType = structType.add("value", DataTypes.StringType, false);

        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        Dataset<Row> momentUpdates = records.groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING())
                .mapGroupsWithState(stateUpdateFunc, Encoders.tuple(Encoders.INT(), Encoders.DOUBLE(), Encoders.DOUBLE()), encoder);


        /* configure the output stream */
        StreamingQuery writer = momentUpdates
                .writeStream()
                .format("kafka")
                .outputMode("update")
                .option("kafka.bootstrap.servers", brokers)
                .option("topic", outtopic)
                .option("checkpointLocation", "/tmp")
                .start();

        /* begin processing the input and output topics */
        try {
            writer.awaitTermination();
        } catch (StreamingQueryException e) {
            spark.log().error("Exception while waiting for query to end {}.", e.getMessage(), e);
        }


    }
}
