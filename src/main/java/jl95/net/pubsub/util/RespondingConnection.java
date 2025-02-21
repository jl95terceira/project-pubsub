package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;
import jl95.net.io.Ios;
import jl95.net.pubsub.Subscription;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.MemberHello;
import jl95.net.pubsub.protocol.Publication;
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

public class RespondingConnection {

    private class MemberIf {

        public final TypeSwitchedResponderIf<JsonValue, Void> switchedResponder;

        public MemberIf(Ios ios) {

            this.switchedResponder = ResponderAdaptersCollection.asPostResponder(TypeSwitchedResponder.fromIo(ios));
            switchedResponder.adaptedRequest(MessageDeserializer.get(PublicationJsonSerdes::fromJson))
                             .addCase       (MessageType.PUBLISH.value,
                                             x -> { pubReqHandler.apply(x);
                                            return null; });
            switchedResponder.adaptedRequest(MessageDeserializer.get(CloseJsonSerdes::fromJson))
                             .addCase       (MessageType.REQ_CLOSE.value,
                                             x -> { closeReqHandler.apply(x);
                                            return null; });
            for (var t: I(
                tuple(MessageType.REQ_SUBSCRIPTION_BY_LIST .value, function(SubscriptionByListJsonSerdes ::fromJson)),
                tuple(MessageType.REQ_SUBSCRIPTION_BY_REGEX.value, function(SubscriptionByRegexJsonSerdes::fromJson)),
                tuple(MessageType.REQ_SUBSCRIPTION_TO_ALL  .value, function(SubscriptionToAllJsonSerdes  ::fromJson)),
                tuple(MessageType.REQ_SUBSCRIPTION_TO_NONE .value, function(SubscriptionToNoneJsonSerdes ::fromJson))
            )) {
                switchedResponder.adaptedRequest(MessageDeserializer.get(t.a2))
                            .addCase       (t.a1, x -> { subReqHandler.apply(x);
                                                      return null; });

            }
        }
    }

    private final Socket   socket;
    private final MemberIf memberIf;
    private       Function1<Boolean, Message<Close>>                  closeReqHandler = x -> { throw new AssertionError(); };
    private       Function1<Boolean, Message<? extends Subscription>> subReqHandler   = x -> { throw new AssertionError(); };
    private       Function1<Boolean, Message<Publication>>            pubReqHandler   = x -> { throw new AssertionError(); };

    public RespondingConnection(Socket socket) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        this.memberIf       = new MemberIf(ios);
    }

    public final Awaitable<Void> startRespond      () {

        return memberIf.switchedResponder.start();
    }
    public final Awaitable<Void> stopRespond       () {

        return memberIf.switchedResponder.stop();
    }
    public final Boolean         isResponding      () {

        return memberIf.switchedResponder.isRunning();
    }
    public final void            setCloseReqHandler(Function1<Boolean, Message<Close>> h) {
        closeReqHandler = h;}
    public final void            setSubReqHandler  (Function1<Boolean, Message<? extends Subscription>> h) {
        subReqHandler = h;}
    public final void            setPubReqHandler  (Function1<Boolean, Message<Publication>> h) {
        pubReqHandler = h;}
    public final Socket          getSocket         () { return socket; }
    public final void            close             () {
        if (isResponding()) {
            stopRespond();
        }
        uncheck(getSocket()::close);
    }
}
