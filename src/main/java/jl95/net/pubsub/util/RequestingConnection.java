package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.function;
import static jl95.lang.SuperPowers.tuple;
import static jl95.lang.SuperPowers.uncheck;

import java.net.Socket;
import java.util.concurrent.*;

import javax.json.JsonValue;

import jl95.lang.Awaitable;
import jl95.lang.variadic.Function1;
import jl95.net.io.Ios;
import jl95.net.io.Sender;
import jl95.net.io.SenderIf;
import jl95.net.io.collections.SenderAdaptersCollections;
import jl95.net.pubsub.Subscription;
import jl95.net.pubsub.protocol.Close;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.serdes.MessageDeserializer;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.CloseJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;
import jl95.net.rpc.collections.ResponderAdaptersCollection;
import jl95.net.rpc.switched.TypeSwitchedResponder;
import jl95.net.rpc.switched.TypeSwitchedResponderIf;

public class RequestingConnection {

    private class MemberIf {

        public final SenderIf<Message<Publication>>           pubSender;

        public MemberIf(Ios ios) {

            var sender     = Sender  .of(ios.getOutputStream());
            this.pubSender = SenderAdaptersCollections.asJsonSender (sender)
                                                      .adaptedSender(MessageSerializer.get(PublicationJsonSerdes::toJson));
        }
    }

    private final BlockingQueue<Message<Publication>>
                            queue          = new ArrayBlockingQueue<>(20);
    private final ThreadPoolExecutor
                            pool           = new ScheduledThreadPoolExecutor(1);
    private      CompletableFuture<Void>
                            queueStopFuture;
    private      Boolean    queueIsOn      = false;
    private      Boolean    queueToStop    = false;

    private final Socket                         socket;
    private final MemberIf                       memberIf;
    private       Subscription                   subscription = (topic) -> false;

    public RequestingConnection(Socket socket) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        this.memberIf       = new MemberIf(ios);
    }

    synchronized public final void            startPubQueue    () {
        if (queueIsOn) { throw new IllegalStateException(); };
        queueToStop     = false;
        queueStopFuture = new CompletableFuture<>();
        pool.execute(() -> {
            while (!queueToStop) {
                var pub = uncheck(() -> queue.poll(125L, TimeUnit.MILLISECONDS));
                if (pub == null) continue;
                memberIf.pubSender.send(pub);
            }
            queueIsOn = false;
            queueStopFuture.complete(null);
        });
        queueIsOn = true;
    }
    synchronized public final Awaitable<Void> stopPubQueue     () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return Awaitable.of(queueStopFuture);
    }
    synchronized public final Boolean         isPubQueueRunning() { return queueIsOn; }

    public final void           pub             (Message<Publication> pubMsg) {

        if (!isPubQueueRunning()) {
            startPubQueue();

        }
        queue.add(pubMsg);
    }
    public final Socket         getSocket       () { return socket; }
    public final Subscription   getSubscription () { return subscription; }
    public final void           setSubscription (Subscription s) { subscription = s; }
    public final void           close           () {
        if (isPubQueueRunning()) {
            stopPubQueue().await();
        }
        uncheck(getSocket()::close);
    }
}
