package jl95.net.pubsub;

import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method2;
import jl95.util.UVoidFuture;

public interface ConsumerIf<C> {

    void        consume         ();
    UVoidFuture consumeStop     ();
    Boolean     isConsuming     ();
    void        onConsumed      (Method2<String, C> pubCallback);

    default void consume(Method2<String, C> pubCallback) {
        onConsumed(pubCallback);
        consume();
    }
    default <C2> ConsumerIf<C2> adaptedConsumer(Function1<C2, C> consumptionAdapter) {
        return new ConsumerIf<>() {
            @Override public void        consume() {
                ConsumerIf.this.consume();
            }
            @Override public UVoidFuture consumeStop() {
                return ConsumerIf.this.consumeStop();
            }
            @Override public Boolean     isConsuming() {
                return ConsumerIf.this.isConsuming();
            }
            @Override public void        onConsumed(Method2<String, C2> pubCallback) {
                ConsumerIf.this.onConsumed((topicName, data) -> {
                    var adaptedData = consumptionAdapter.apply(data);
                    pubCallback.accept(topicName, adaptedData);
                });
            }
        };
    }
}
