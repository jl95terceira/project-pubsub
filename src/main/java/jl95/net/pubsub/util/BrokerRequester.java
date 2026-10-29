package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import jl95.lang.variadic.*;
import jl95.net.io.IOStreamSupplier;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.SubscriptionByList;
import jl95.net.pubsub.protocol.SubscriptionByRegex;
import jl95.net.pubsub.protocol.SubscriptionToAll;
import jl95.net.pubsub.protocol.SubscriptionToNone;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByListJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionByRegexJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToAllJsonSerdes;
import jl95.net.pubsub.util.serdes.protocol.SubscriptionToNoneJsonSerdes;
import jl95.net.rpc.Requester;
import jl95.net.rpc.collections.TypedRequesterAdaptersCollection;
import jl95.net.rpc.switched.TypedRequesterImpl;
import jl95.util.UVoidFuture;

public class BrokerRequester {

    private static class Rpc {

        public final Requester<Message<Publication>,         Void> pubSender;
        public final Requester<Message<SubscriptionByList>,  Void> subListSender;
        public final Requester<Message<SubscriptionByRegex>, Void> subRegexSender;
        public final Requester<Message<SubscriptionToAll>,   Void> subAllSender;
        public final Requester<Message<SubscriptionToNone>,  Void> subNoneSender;

        public Rpc(IOStreamSupplier ios) {

            var typedRequester = TypedRequesterAdaptersCollection.asPostRequester(TypedRequesterImpl.fromIo(ios));
            this.pubSender      = typedRequester.getFunction   (MessageType.PUBLISH                  .value)
                                                .adaptedRequest(MessageSerializer.get(PublicationJsonSerdes        ::toJson));
            this.subListSender  = typedRequester.getFunction   (MessageType.REQ_SUBSCRIPTION_BY_LIST .value)
                                                .adaptedRequest(MessageSerializer.get(SubscriptionByListJsonSerdes ::toJson));
            this.subRegexSender = typedRequester.getFunction   (MessageType.REQ_SUBSCRIPTION_BY_REGEX.value)
                                                .adaptedRequest(MessageSerializer.get(SubscriptionByRegexJsonSerdes::toJson));
            this.subAllSender   = typedRequester.getFunction   (MessageType.REQ_SUBSCRIPTION_TO_ALL  .value)
                                                .adaptedRequest(MessageSerializer.get(SubscriptionToAllJsonSerdes  ::toJson));
            this.subNoneSender  = typedRequester.getFunction   (MessageType.REQ_SUBSCRIPTION_TO_NONE .value)
                                                .adaptedRequest(MessageSerializer.get(SubscriptionToNoneJsonSerdes ::toJson));
        }
    }
    private class CallbacksImpl implements Callbacks {

        @Override public void onPub     (Message<Publication>         msg) { rpc.pubSender     .apply(msg); }
        @Override public void onSubList (Message<SubscriptionByList>  msg) { rpc.subListSender .apply(msg); }
        @Override public void onSubRegex(Message<SubscriptionByRegex> msg) { rpc.subRegexSender.apply(msg); }
        @Override public void onSubAll  (Message<SubscriptionToAll>   msg) { rpc.subAllSender  .apply(msg); }
        @Override public void onSubNone (Message<SubscriptionToNone>  msg) { rpc.subNoneSender .apply(msg); }
    }

    private final BlockingQueue<Method1<Callbacks>> queue = new LinkedBlockingQueue<>();
    private final ThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(1);
    private CompletableFuture<Void> queueStopFuture;
    private Boolean queueIsOn = false;
    private Boolean queueToStop = false;

    private final Socket socket;
    private final Rpc rpc;
    private final CallbacksImpl cbsImpl = new CallbacksImpl();

    public interface Callbacks {
        void onPub     (Message<Publication>         msg);
        void onSubList (Message<SubscriptionByList>  msg);
        void onSubRegex(Message<SubscriptionByRegex> msg);
        void onSubAll  (Message<SubscriptionToAll>   msg);
        void onSubNone (Message<SubscriptionToNone>  msg);
    }

    public BrokerRequester(Socket socket) {
        this.socket         = socket;
        var ios             = IOStreamSupplier.fromSocketLazy(socket);
        this.rpc = new Rpc(ios);
    }

    public synchronized final void        startQueueLoop    () {
        if (queueIsOn) { throw new IllegalStateException(); };
        queueToStop     = false;
        queueStopFuture = new CompletableFuture<>();
        pool.execute(() -> {
            while (!queueToStop) {
                var cbCaller = uncheck(() -> queue.poll(125L, TimeUnit.MILLISECONDS));
                if (cbCaller == null) continue;
                cbCaller.accept(cbsImpl);
            }
            queueIsOn = false;
            queueStopFuture.complete(null);
        });
        queueIsOn = true;
    }
    public synchronized final UVoidFuture stopQueueLoop     () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return UVoidFuture.of(queueStopFuture);
    }
    public synchronized final Boolean     isQueueLoopRunning() { return queueIsOn; }
    public final void   addToQueue(Method1<Callbacks> cbCaller) {

        if (!isQueueLoopRunning()) {
            startQueueLoop();

        }
        queue.add(cbCaller);
    }
    public final Socket getSocket () { return socket; }
    public final void   close     () {
        if (isQueueLoopRunning()) {
            stopQueueLoop().get();
        }
        uncheck(getSocket()::close);
    }
}
