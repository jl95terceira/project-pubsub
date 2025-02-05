package jl95.pubsub;

import static jl95.lang.SuperPowers.*;

import java.util.concurrent.CompletableFuture;

import jl95.lang.Ref;
import jl95.net.util.Util;
import jl95.pubsub.util.Defaults;

public class Test {

    public Server server;

    private Client getClient() {
        return new Client(Defaults.serverAddr, Client.Options.defaults());
    }

    @org.junit.Before
    public void setUp() {
        server = new Server(Util.getSimpleServerSocket(Defaults.serverAddr, Defaults.serverAcceptTimeoutMs), Server.Options.defaults());
        server.startAccept();
    }
    @org.junit.After
    public void tearDown() {
        server.stopAcceptAwait();
        uncheck(() -> server.getNetServer().getSocket().close());
    }

    @org.junit.Test
    public void test() {}
    @org.junit.Test
    public void test2() {}
    @org.junit.Test
    public void testConnectAndClose() {
        var client  = getClient();
        var client2 = getClient();
        client .close();
        client2.close();
    }
    @org.junit.Test
    public void testPubSub() {
        var publisher  = getClient();
        var subscriber = getClient();
        var msgFuture  = new CompletableFuture<String>();
        subscriber.onConsumed(msg -> {
            msgFuture.complete(new String(msg.data));
        });
        subscriber.subscribeByList(I("foo"));
        publisher.produce("foo", "BAR".getBytes());
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }
    @org.junit.Test
    public void testPubNoSub() {
        var publisher  = getClient();
        var subscriber = getClient();
        var msgFuture  = new CompletableFuture<String>();
        subscriber.onConsumed(msg -> {
            msgFuture.complete(new String(msg.data));
        });
        publisher.produce("foo", "BAR".getBytes());
        sleep(125);
        org.junit.Assert.assertFalse(msgFuture.isDone());
    }
    @org.junit.Test
    public void testPubNoSubThenSub() {
        var publisher  = getClient();
        var subscriber = getClient();
        var msgFuture  = new CompletableFuture<String>();
        subscriber.onConsumed(msg -> {
            msgFuture.complete(new String(msg.data));
        });
        publisher.produce("foo", "BAR".getBytes());
        org.junit.Assert.assertFalse(msgFuture.isDone());
        subscriber.subscribeByList(I("foo"));
        publisher.produce("foo", "BAR".getBytes());
        sleep(125);
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }
}
