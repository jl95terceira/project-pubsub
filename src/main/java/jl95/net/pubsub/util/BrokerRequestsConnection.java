package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.uncheck;

import java.net.Socket;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;
import jl95.net.io.Ios;
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
import jl95.net.rpc.collections.ResponderAdaptersCollection;
import jl95.net.rpc.switched.TypeSwitchedResponder;
import jl95.net.rpc.switched.TypeSwitchedResponderIf;

public class BrokerRequestsConnection {

    private class MemberIf {

        public final TypeSwitchedResponderIf<JsonValue, Void> switchedResponder;

        public MemberIf(Ios ios) {

            this.switchedResponder = ResponderAdaptersCollection.asPostResponder(TypeSwitchedResponder.fromIo(ios));
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

    private final Socket   socket;
    private final MemberIf memberIf;
    public Function1<Boolean, Message<Close>>               closeReqHandler    = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionByList>>  subListReqHandler  = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionByRegex>> subRegexReqHandler = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionToAll>>   subAllReqHandler   = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<SubscriptionToNone>>  subNoneReqHandler  = x -> { throw new AssertionError(); };
    public Function1<Boolean, Message<Publication>>         pubReqHandler      = x -> { throw new AssertionError(); };

    public BrokerRequestsConnection(Socket socket) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        this.memberIf       = new MemberIf(ios);
    }

    public final Awaitable<Void> startRespond         () {

        return memberIf.switchedResponder.start();
    }
    public final Awaitable<Void> stopRespond          () {

        return memberIf.switchedResponder.stop();
    }
    public final Boolean         isResponding         () {

        return memberIf.switchedResponder.isRunning();
    }
    public final Socket          getSocket            () { return socket; }
    public final void            close                () {
        if (isResponding()) {
            stopRespond();
        }
        uncheck(getSocket()::close);
    }
}
