package bonesbrigade.service.moments;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

public class Main {

    private static final String BROKER = "localhost:9092";
    private static final String TOPIC = "bones-brigade";

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("KafkaSparkOpenShiftJava")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        /* configure the operations to read the input topic */
        Dataset<Row> records = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BROKER)
                .option("subscribe", TOPIC)
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

                    return RowFactory.create(aggregate._2(), aggregate._3() / (double) aggregate._1());
                };

        StructType structType = new StructType();
        structType = structType.add("mean", DataTypes.DoubleType, false);
        structType = structType.add("variance", DataTypes.DoubleType, false);

        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        Dataset<Row> sessionUpdates = records.groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING())
                .mapGroupsWithState(stateUpdateFunc, Encoders.tuple(Encoders.INT(), Encoders.DOUBLE(), Encoders.DOUBLE()), encoder);


        /* configure the output stream */
        StreamingQuery query = sessionUpdates.writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .option("truncate", false)
                .trigger(Trigger.ProcessingTime(1000))
                .start();

        /* begin processing the input and output topics */
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            spark.log().error("Exception while waiting for query to end {}.", e.getMessage(), e);
        }


    }
}
