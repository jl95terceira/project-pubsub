package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.uncheck;

import java.net.Socket;
import java.util.concurrent.*;

import jl95.lang.variadic.Method0;
import jl95.net.io.Ios;
import jl95.net.io.managed.ManagedIos;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.protocol.PublicationAcceptanceRequest;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationAcceptanceRequestJsonSerdes;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.rpc.RequesterIf;
import jl95.net.rpc.collections.TypedRequesterAdaptersCollection;
import jl95.net.rpc.switched.TypedRequester;
import jl95.util.UVoidFuture;

public class MemberResponsesConnection {

    private static class MemberIf {

        public final RequesterIf<Message<Publication>, Void> pubSender;
        public final RequesterIf<Message<PublicationAcceptanceRequest>, Boolean> parSender;

        public MemberIf(ManagedIos ios) {

            var jsonTypedRequester = TypedRequester.fromManagedIo(ios);
            this.pubSender = TypedRequesterAdaptersCollection.asPostRequester(jsonTypedRequester)
                .adaptedRequest (MessageSerializer.get(PublicationJsonSerdes::toJson))
                .getFunction(MessageType.PUBLISH                .value);
            this.parSender = jsonTypedRequester
                .adaptedRequest (MessageSerializer.get(PublicationAcceptanceRequestJsonSerdes::toJson))
                .adaptedResponse(SerdesDefaults.boolFromJson)
                .getFunction(MessageType.PUBLISH_ACCEPT_REQUEST .value);
        }
    }

    private final BlockingQueue<Method0>
                                     queue          = new ArrayBlockingQueue<>(20);
    private final ThreadPoolExecutor pool           = new ScheduledThreadPoolExecutor(1);
    private final Socket             socket;
    private final MemberIf           memberIf;
    private       CompletableFuture<Void>
                                     queueStopFuture;
    private       Boolean            queueIsOn      = false;
    private       Boolean            queueToStop    = false;

    public MemberResponsesConnection(Socket socket) {
        this.socket         = socket;
        var ios             = Ios.fromSocketLazy(socket);
        this.memberIf       = new MemberIf(ManagedIos.of(ios));
    }

    private void processPub(Message<Publication> pubMsg) {

        var parMsg = new Message<PublicationAcceptanceRequest>();
        parMsg.id       = pubMsg.id;
        parMsg.memberId = pubMsg.memberId;
        Boolean accepted;
        try {
            accepted = true;//memberIf.parSender.apply(parMsg).get(2, TimeUnit.SECONDS); //TODO: fix this (blocking)
        }
        //catch (TimeoutException | InterruptedException | ExecutionException ex) {
        catch (Exception ex) {
            accepted = false;
        }
        if (accepted) {
            memberIf.pubSender.apply(pubMsg);
        }
    }

    synchronized public final void        startQueueLoop   () {
        if (queueIsOn) { throw new IllegalStateException(); };
        queueToStop     = false;
        queueStopFuture = new CompletableFuture<>();
        pool.execute(() -> {
            while (!queueToStop) {
                var queuedAction = uncheck(() -> queue.poll(125L, TimeUnit.MILLISECONDS));
                if (queuedAction != null) {
                    queuedAction.accept();
                }
            }
            queueIsOn = false;
            queueStopFuture.complete(null);
        });
        queueIsOn = true;
    }
    synchronized public final UVoidFuture stopQueueLoop    () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return UVoidFuture.of(queueStopFuture);
    }
    synchronized public final Boolean     isPubQueueRunning() { return queueIsOn; }

    public final void   sendPub   (Message<Publication> pubMsg) {

        if (!isPubQueueRunning()) {
            startQueueLoop();

        }
        queue.add(() -> processPub(pubMsg));
    }
    public final Socket getSocket () { return socket; }
    public final void   close     () {
        if (isPubQueueRunning()) {
            stopQueueLoop().get();
        }
        uncheck(getSocket()::close);
    }
}
