package jl95.net.pubsub;

import static jl95.lang.SuperPowers.I;
import static jl95.lang.SuperPowers.sleep;
import static jl95.lang.SuperPowers.uncheck;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import jl95.net.io.util.Util;
import jl95.net.pubsub.collections.MemberAdaptersCollection;
import jl95.net.pubsub.util.Defaults;

public class Test {

    public Instant t0;
    public Broker server;
    public Member client;
    public Member client2;

    @org.junit.Before
    public void setUp() {
        t0 = Instant.now();
        server = new Broker(Util.getSimpleServerSocket(Defaults.serverAddr, Defaults.serverAcceptTimeoutMs));
        server.startAccept().await();
        client  = new Member(Defaults.serverAddr);
        client2 = new Member(Defaults.serverAddr);
    }
    @org.junit.After
    public void tearDown() {
        client.close();
        client2.close();
        server.stopAccept().await();
        uncheck(() -> server.getNetServer().getSocket().close());
    }

    @org.junit.Test
    public void testConnectAndClose() {
    }
    @org.junit.Test
    public void testPubSub() {
        for (var cclient: I(client, client2)) {
            var msgFuture = new CompletableFuture<String>();
            MemberAdaptersCollection.getStringConsumer(cclient).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            cclient.subscribeByList(I("foo"));
            MemberAdaptersCollection.getStringProducer(client).produce("foo", "BAR");
            sleep(125);
            org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
        }
    }
    @org.junit.Test
    public void testPubNoSub() {
        for (var cclient: I(client, client2)) {
            var msgFuture = new CompletableFuture<String>();
            MemberAdaptersCollection.getStringConsumer(cclient).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            MemberAdaptersCollection.getStringProducer(client).produce("foo", "BAR");
            sleep(125);
            org.junit.Assert.assertFalse(msgFuture.isDone());
        }
    }
    @org.junit.Test
    public void testPubNoSubThenSub() {
        for (var cclient: I(client, client2)) {
            var msgFuture = new CompletableFuture<String>();
            MemberAdaptersCollection.getStringConsumer(cclient).consume((topic, payload) -> {
                msgFuture.complete(payload);
            });
            sleep(125);
            MemberAdaptersCollection.getStringProducer(client).produce("foo", "BAR");
            org.junit.Assert.assertFalse(msgFuture.isDone());
            cclient.subscribeByList(I("foo"));
            MemberAdaptersCollection.getStringProducer(client).produce("foo", "BAR");
            sleep(125);
            org.junit.Assert.assertEquals("BAR", uncheck(() -> msgFuture.get()));
        }
    }
}
