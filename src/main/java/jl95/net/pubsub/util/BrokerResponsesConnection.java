package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.*;

import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import jl95.lang.*;
import jl95.lang.variadic.*;
import jl95.net.io.Ios;
import jl95.net.io.Sender;
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
import jl95.net.rpc.RequesterIf;
import jl95.net.rpc.collections.RequesterAdaptersCollection;
import jl95.net.rpc.switched.TypedRequester;

public class BrokerResponsesConnection {

    private static class MemberIf {

        public final RequesterIf<Message<Publication>,         Void> pubSender;
        public final RequesterIf<Message<SubscriptionByList>,  Void> subListSender;
        public final RequesterIf<Message<SubscriptionByRegex>, Void> subRegexSender;
        public final RequesterIf<Message<SubscriptionToAll>,   Void> subAllSender;
        public final RequesterIf<Message<SubscriptionToNone>, Void>  subNoneSender;

        public MemberIf(Ios ios) {

            var typedRequester = RequesterAdaptersCollection.asPostRequester(TypedRequester.fromIo(ios));
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

        @Override public void onPub     (Message<Publication>         msg) { memberIf.pubSender     .apply(msg); }
        @Override public void onSubList (Message<SubscriptionByList>  msg) { memberIf.subListSender .apply(msg); }
        @Override public void onSubRegex(Message<SubscriptionByRegex> msg) { memberIf.subRegexSender.apply(msg); }
        @Override public void onSubAll  (Message<SubscriptionToAll>   msg) { memberIf.subAllSender  .apply(msg); }
        @Override public void onSubNone (Message<SubscriptionToNone>  msg) { memberIf.subNoneSender .apply(msg); }
    }

    private final BlockingQueue<Method1<Callbacks>>
                            queue          = new LinkedBlockingQueue<>();
    private final ThreadPoolExecutor
                            pool           = new ScheduledThreadPoolExecutor(1);
    private      CompletableFuture<Void>
                            queueStopFuture;
    private      Boolean    queueIsOn      = false;
    private      Boolean    queueToStop    = false;

    private final Socket        socket;
    private final MemberIf      memberIf;
    private final CallbacksImpl cbsImpl = new CallbacksImpl();

    public interface Callbacks {
        void onPub     (Message<Publication>         msg);
        void onSubList (Message<SubscriptionByList>  msg);
        void onSubRegex(Message<SubscriptionByRegex> msg);
        void onSubAll  (Message<SubscriptionToAll>   msg);
        void onSubNone (Message<SubscriptionToNone>  msg);
    }

    public BrokerResponsesConnection(Socket socket) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        this.memberIf       = new MemberIf(ios);
    }

    synchronized public final void          startQueueLoop    () {
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
    synchronized public final VoidAwaitable stopQueueLoop     () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return VoidAwaitable.of(queueStopFuture);
    }
    synchronized public final Boolean       isQueueLoopRunning() { return queueIsOn; }

    public final void           addToQueue(Method1<Callbacks> cbCaller) {

        if (!isQueueLoopRunning()) {
            startQueueLoop();

        }
        queue.add(cbCaller);
    }
    public final Socket         getSocket () { return socket; }
    public final void           close     () {
        if (isQueueLoopRunning()) {
            stopQueueLoop().await();
        }
        uncheck(getSocket()::close);
    }
}
