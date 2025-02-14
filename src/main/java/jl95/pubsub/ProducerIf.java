package jl95.pubsub;

import jl95.lang.variadic.Function1;

public interface ProducerIf<P> {

    void produce(String topicName,
                 P      data);

    default <P2> ProducerIf<P2> adaptedProducer(Function1<P, P2> productionAdapter) {
        return (topicName, data) -> {
            var adaptedData = productionAdapter.apply(data);
            ProducerIf.this.produce(topicName, adaptedData);
        };
    }
}
