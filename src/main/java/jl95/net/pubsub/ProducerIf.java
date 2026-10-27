package jl95.net.pubsub;

import jl95.lang.variadic.Function1;
import jl95.util.UVoidFuture;

public interface ProducerIf<P> {

    UVoidFuture produce(String topicName, P data);

    default <P2> ProducerIf<P2> adaptedProducer(Function1<P, P2> productionAdapter) {
        return (topicName, data) -> {
            var adaptedData = productionAdapter.apply(data);
            return ProducerIf.this.produce(topicName, adaptedData);
        };
    }
}
