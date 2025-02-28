package jl95.net.pubsub.util;

import static jl95.lang.SuperPowers.function;
import static jl95.lang.SuperPowers.tuple;
import static jl95.lang.SuperPowers.uncheck;

import java.net.Socket;
import java.util.concurrent.*;

import jl95.lang.Awaitable;
import jl95.net.io.Ios;
import jl95.net.io.Sender;
import jl95.net.pubsub.protocol.Publication;
import jl95.net.pubsub.util.serdes.MessageSerializer;
import jl95.net.pubsub.util.serdes.PublicationJsonSerdes;
import jl95.net.rpc.Requester;
import jl95.net.rpc.RequesterIf;
import jl95.net.rpc.collections.RequesterAdaptersCollection;

public class MemberResponsesConnection {

    private class MemberIf {

        public final RequesterIf<Message<Publication>, Void> pubSender;

        public MemberIf(Ios ios) {

            var sender     = Sender  .of(ios.getOutputStream());
            this.pubSender = RequesterAdaptersCollection.asPostRequester(Requester.fromIo(ios))
                                                      .adaptedRequest(MessageSerializer.get(PublicationJsonSerdes::toJson));
        }
    }

    private final BlockingQueue<Message<Publication>>
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
        this.memberIf       = new MemberIf(ios);
    }

    synchronized public final void            startQueueLoop   () {
        if (queueIsOn) { throw new IllegalStateException(); };
        queueToStop     = false;
        queueStopFuture = new CompletableFuture<>();
        pool.execute(() -> {
            while (!queueToStop) {
                var pub = uncheck(() -> queue.poll(125L, TimeUnit.MILLISECONDS));
                if (pub == null) continue;
                memberIf.pubSender.apply(pub);
            }
            queueIsOn = false;
            queueStopFuture.complete(null);
        });
        queueIsOn = true;
    }
    synchronized public final Awaitable<Void> stopQueueLoop    () {
        if (!queueIsOn) { throw new IllegalStateException(); };
        queueToStop = true;
        return Awaitable.of(queueStopFuture);
    }
    synchronized public final Boolean         isPubQueueRunning() { return queueIsOn; }

    public final void   addToQueue(Message<Publication> pubMsg) {

        if (!isPubQueueRunning()) {
            startQueueLoop();

        }
        queue.add(pubMsg);
    }
    public final Socket getSocket () { return socket; }
    public final void   close     () {
        if (isPubQueueRunning()) {
            stopQueueLoop().await();
        }
        uncheck(getSocket()::close);
    }
}
