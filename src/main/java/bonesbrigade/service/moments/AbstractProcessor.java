package bonesbrigade.service.moments;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;

import java.io.Serializable;

public abstract class AbstractProcessor<K, V, S, R> implements Serializable {

    abstract R processFunction(K key, Optional<V> value, State<S> state);

}
