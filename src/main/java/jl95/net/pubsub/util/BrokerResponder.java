package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.uncheck;

import java.net.Socket;

import javax.json.JsonValue;

import jl95.lang.variadic.Function1;
import jl95.net.io.IOStreamSupplier;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.SubscriptionByList;
import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.protocol.SubscriptionToAll;
import jl95.net.pubsub.protocol.SubscriptionToNone;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.CloseJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;
import jl95.net.rpc.collections.TypeSwitchedResponderAdaptersCollection;
import jl95.net.rpc.switched.TypeSwitchedResponderImpl;
import jl95.net.rpc.switched.TypeSwitchedResponder;
import jl95.util.UVoidFuture;

public class BrokerResponder {

    private class Rpc {

        public final TypeSwitchedResponder<JsonValue, Void> switchedResponder;

        public Rpc(IOStreamSupplier ios) {

            this.switchedResponder = TypeSwitchedResponderAdaptersCollection.asPostResponder(TypeSwitchedResponderImpl.fromIo(ios));
            switchedResponder
                .adaptedRequest(MessageDeserializer.get(PublicationJsonSerdes::fromJson))
                .addCase(MessageType.PUBLISH.value, x -> {
                    pubReqHandler.apply(x);
                    return null;
                    });
            switchedResponder
                .adaptedRequest(MessageDeserializer.get(CloseJsonSerdes::fromJson))
                .addCase(MessageType.REQ_CLOSE.value, x -> {
                    closeReqHandler.apply(x);
                    return null;
                });
            switchedResponder
                .adaptedRequest(MessageDeserializer.get(SubscriptionByListJsonSerdes ::fromJson))
                .addCase(MessageType.REQ_SUBSCRIPTION_BY_LIST.value, x -> {
                    subListReqHandler.apply(x);
                    return null;
                });
            switchedResponder
                .adaptedRequest(MessageDeserializer.get(SubscriptionByRegexJsonSerdes ::fromJson))
                .addCase(MessageType.REQ_SUBSCRIPTION_BY_REGEX.value, x -> {
                    subRegexReqHandler.apply(x);
                    return null;
                });
            switchedResponder
                .adaptedRequest(MessageDeserializer.get(SubscriptionToAllJsonSerdes ::fromJson))
                .addCase(MessageType.REQ_SUBSCRIPTION_TO_ALL.value, x -> {
                    subAllReqHandler.apply(x);
                    return null;
                });
            switchedResponder
                .adaptedRequest(MessageDeserializer.get(SubscriptionToNoneJsonSerdes ::fromJson))
                .addCase(MessageType.REQ_SUBSCRIPTION_TO_NONE.value, x -> {
                    subNoneReqHandler.apply(x);
                    return null;
                });
        }
    }

    private final Socket socket;
    private final Rpc rpc;
    public Function1<Boolean, Message<Close>>               closeReqHandler    = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionByList>>  subListReqHandler  = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionByRegex>> subRegexReqHandler = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionToAll>>   subAllReqHandler   = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionToNone>>  subNoneReqHandler  = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<Publication>>         pubReqHandler      = x -> { throw new AssertionError(); };

    public BrokerResponder(Socket socket) {
        this.socket         = socket;
        var ios             = IOStreamSupplier.fromSocketLazy(socket);
        this.rpc = new Rpc(ios);
    }

    public final UVoidFuture startRespond         () {

        return rpc.switchedResponder.start();
    }
    public final UVoidFuture stopRespond          () {

        return rpc.switchedResponder.stop();
    }
    public final Boolean     isResponding         () {

        return rpc.switchedResponder.isRunning();
    }
    public final Socket      getSocket            () { return socket; }
    public final void        close                () {
        if (isResponding()) {
            stopRespond();
        }
        uncheck(getSocket()::close);
    }
}
