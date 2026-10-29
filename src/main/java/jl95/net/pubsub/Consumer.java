package jl95.net.pubsub;

import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method2;
import jl95.util.UVoidFuture;

public interface Consumer<C> {

    void        consume    ();
    UVoidFuture consumeStop();
    Boolean     isConsuming();
    void        onConsumed (Method2<String, C> pubCallback);

    default void consume(Method2<String, C> pubCallback) {
        onConsumed(pubCallback);
        consume();
    }
    default <C2> Consumer<C2> adaptedConsumer(Function1<C2, C> consumptionAdapter) {
        return new Consumer<>() {
            @Override public void        consume() {
                Consumer.this.consume();
            }
            @Override public UVoidFuture consumeStop() {
                return Consumer.this.consumeStop();
            }
            @Override public Boolean     isConsuming() {
                return Consumer.this.isConsuming();
            }
            @Override public void        onConsumed(Method2<String, C2> pubCallback) {
                Consumer.this.onConsumed((topicName, data) -> {
                    var adaptedData = consumptionAdapter.apply(data);
                    pubCallback.accept(topicName, adaptedData);
                });
            }
        };
    }
}
