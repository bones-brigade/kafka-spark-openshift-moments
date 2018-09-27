package bonesbrigade.service.moments;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("KafkaMoments").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        jssc.sparkContext().setLogLevel("ERROR");

       jssc.checkpoint("/tmp");

        HashSet<String> topicsSet = new HashSet<>();
        topicsSet.add("bones-brigade");

        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        Function3<String, Optional<String>, State<Aggregate>, Tuple2<Double, Double>> mappingFunction = (category, value, state) -> {
            Aggregate aggregate;
            if (state.exists()) {
                aggregate = state.get();
            } else {
                aggregate = new Aggregate();
            }
            double val = 0.0;

            if (value.isPresent()) {
                val = Double.parseDouble(value.get());
            }
            int count = 1 + aggregate.count;
            double delta = val - aggregate.mean;
            double mean = aggregate.mean + delta / (double) count;
            double delta2 = val - mean;
            double squaredDistance = aggregate.squaredDistance + delta * delta2;
            Aggregate newAggregate = new Aggregate(count, mean, squaredDistance);
            state.update(newAggregate);
            return new Tuple2<>(mean, squaredDistance/(double)count);

        };

        JavaMapWithStateDStream<String, String, Aggregate, Tuple2<Double, Double>> aggregateStream = messages.mapWithState(StateSpec.function(mappingFunction));
        aggregateStream.foreachRDD((values) -> values.foreach(e -> System.out.println("Mean = " + e._1.toString() + ", Variance = " + e._2.toString())));

        // Start the computation
        jssc.start();
        jssc.awaitTermination();

    }
}
