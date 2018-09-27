package bonesbrigade.service.moments;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import scala.Tuple2;

public class MomentsProcessor extends AbstractProcessor<String, String, Aggregate, Tuple2<Double, Double> > {

    @Override
    Tuple2<Double, Double> processFunction(String key, Optional<String> value, State<Aggregate> state) {
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
    }
}
