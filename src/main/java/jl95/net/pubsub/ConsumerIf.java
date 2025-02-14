package jl95.net.pubsub;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;
import jl95.lang.variadic.Method2;

public interface ConsumerIf<C> {

    void            consume         ();
    Awaitable<Void> consumeStop     ();
    Boolean         isConsuming     ();
    void            onConsumed      (Method2<String, C> pubCallback);

    default <C2> ConsumerIf<C2> adaptedConsumer(Function1<C2, C> consumptionAdapter) {
        return new ConsumerIf<>() {
            @Override public void consume() {
                ConsumerIf.this.consume();
            }
            @Override public Awaitable<Void> consumeStop() {
                return ConsumerIf.this.consumeStop();
            }
            @Override public Boolean isConsuming() {
                return ConsumerIf.this.isConsuming();
            }
            @Override public void onConsumed(Method2<String, C2> pubCallback) {
                ConsumerIf.this.onConsumed((topicName, data) -> {
                    var adaptedData = consumptionAdapter.apply(data);
                    pubCallback.accept(topicName, adaptedData);
                });
            }
        };
    }
}
