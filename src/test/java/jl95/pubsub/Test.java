package jl95.pubsub;

import static jl95.lang.SuperPowers.*;

import java.util.concurrent.CompletableFuture;

import jl95.net.util.Util;
import jl95.pubsub.util.Defaults;

public class Test {

    public Server server;

    private Client<String, String> getClient() {
        return ClientsCollection.getStringClient(Defaults.serverAddr);
    }

    @org.junit.Before
    public void setUp() {
        server = new Server(Util.getSimpleServerSocket(Defaults.serverAddr, Defaults.serverAcceptTimeoutMs));
        server.startAccept().await();
    }
    @org.junit.After
    public void tearDown() {
        server.stopAccept().await();
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
        subscriber.onConsumed((topic, payload) -> {
            msgFuture.complete(payload);
        });
        subscriber.subscribeByList(I("foo"));
        sleep(125);
        publisher.produce("foo", "BAR");
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }
    @org.junit.Test
    public void testPubNoSub() {
        var publisher  = getClient();
        var subscriber = getClient();
        var msgFuture  = new CompletableFuture<String>();
        subscriber.onConsumed((topic, payload) -> {
            msgFuture.complete(payload);
        });
        publisher.produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertFalse(msgFuture.isDone());
    }
    @org.junit.Test
    public void testPubNoSubThenSub() {
        var publisher  = getClient();
        var subscriber = getClient();
        var msgFuture  = new CompletableFuture<String>();
        subscriber.onConsumed((topic, payload) -> {
            msgFuture.complete(payload);
        });
        publisher.produce("foo", "BAR");
        org.junit.Assert.assertFalse(msgFuture.isDone());
        subscriber.subscribeByList(I("foo"));
        sleep(125);
        publisher.produce("foo", "BAR");
        sleep(125);
        org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
    }
}
